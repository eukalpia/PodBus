import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_nats/podbus_nats.dart';

const _streamName = 'PODBUS_PLAIN_DART_SERVICE';
const _subject = 'podbus.plain.jobs.smoke';

Future<void> main() async {
  final environment = Platform.environment;
  final natsUrl = environment['PODBUS_NATS_URL'] ?? 'nats://127.0.0.1:4222';
  final port = int.tryParse(environment['PODBUS_PLAIN_PORT'] ?? '') ?? 8088;
  final processed = <String>{};
  final shutdown = Completer<void>();

  final queue = ResilientDurableJobQueue(
    factory: () => NatsJetStreamJobQueue(
      config: NatsMessagingConfig(
        servers: [Uri.parse(natsUrl)],
        connectTimeout: const Duration(seconds: 2),
        requestTimeout: const Duration(seconds: 5),
        jetStream: const NatsJetStreamConfig(
          enabled: true,
          streamName: _streamName,
          subjects: ['podbus.plain.jobs.>'],
          storage: NatsJetStreamStorage.memory,
          consumerConfig: NatsJetStreamConsumerConfig(
            ackWait: Duration(seconds: 10),
            maxDeliver: 10,
            maxAckPending: 256,
          ),
        ),
      ),
      fetchTimeout: const Duration(milliseconds: 100),
      fetchBatchSize: 32,
    ),
    policy: const ReconnectPolicy(
      maxAttempts: 20,
      initialDelay: Duration(milliseconds: 100),
      maxDelay: Duration(seconds: 2),
      recoveryTimeout: Duration(seconds: 30),
      healthCheckInterval: Duration(seconds: 1),
      healthCheckTimeout: Duration(milliseconds: 750),
    ),
  );

  await queue.connect();
  final worker = await queue.worker<Map<String, Object?>>(
    _subject,
    durableName: 'plain-dart-service-v1',
    concurrency: 4,
    handler: (_, payload) async {
      final id = payload['id'];
      if (id is! String || id.isEmpty) {
        throw const FormatException('Plain Dart smoke job requires a string id.');
      }
      processed.add(id);
      stdout.writeln(jsonEncode({'event': 'processed', 'id': id}));
    },
  );

  final server = await HttpServer.bind(InternetAddress.loopbackIPv4, port);
  final serverSubscription = server.listen((request) async {
    try {
      await _handleRequest(request, processed, shutdown);
    } on Object catch (error, stackTrace) {
      stderr.writeln('HTTP handler failed: $error\n$stackTrace');
      if (!request.response.headersSent) {
        request.response.statusCode = HttpStatus.internalServerError;
      }
      await request.response.close();
    }
  });

  final signalSubscriptions = <StreamSubscription<ProcessSignal>>[];
  for (final signal in [ProcessSignal.sigterm, ProcessSignal.sigint]) {
    try {
      signalSubscriptions.add(
        signal.watch().listen((_) {
          if (!shutdown.isCompleted) {
            shutdown.complete();
          }
        }),
      );
    } on UnsupportedError {
      // Signal streams are not available on every platform.
    }
  }

  stdout.writeln(
    jsonEncode({
      'event': 'ready',
      'port': port,
      'transport': 'nats-jetstream',
      'framework': 'plain-dart',
    }),
  );

  try {
    await shutdown.future;
  } finally {
    await server.close(force: false);
    await serverSubscription.cancel();
    await worker.close();
    await queue.close(timeout: const Duration(seconds: 10));
    for (final subscription in signalSubscriptions) {
      await subscription.cancel();
    }
    stdout.writeln(jsonEncode({'event': 'stopped'}));
  }
}

Future<void> _handleRequest(
  HttpRequest request,
  Set<String> processed,
  Completer<void> shutdown,
) async {
  final response = request.response;
  response.headers.contentType = ContentType.json;

  if (request.method == 'GET' && request.uri.path == '/ready') {
    response.statusCode = HttpStatus.ok;
    response.write(jsonEncode({'ready': true}));
    await response.close();
    return;
  }

  if (request.method == 'GET' &&
      request.uri.pathSegments.length == 2 &&
      request.uri.pathSegments.first == 'processed') {
    final id = request.uri.pathSegments[1];
    final found = processed.contains(id);
    response.statusCode = found ? HttpStatus.ok : HttpStatus.notFound;
    response.write(jsonEncode({'processed': found, 'id': id}));
    await response.close();
    return;
  }

  if (request.method == 'POST' && request.uri.path == '/shutdown') {
    response.statusCode = HttpStatus.accepted;
    response.write(jsonEncode({'shuttingDown': true}));
    await response.close();
    scheduleMicrotask(() {
      if (!shutdown.isCompleted) {
        shutdown.complete();
      }
    });
    return;
  }

  response.statusCode = HttpStatus.notFound;
  response.write(jsonEncode({'error': 'not found'}));
  await response.close();
}
