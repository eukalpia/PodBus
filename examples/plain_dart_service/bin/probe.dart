import 'dart:async';
import 'dart:io';

import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_nats/podbus_nats.dart';

const _streamName = 'PODBUS_PLAIN_DART_SERVICE';
const _subject = 'podbus.plain.jobs.smoke';

Future<void> main() async {
  final environment = Platform.environment;
  final natsUrl = environment['PODBUS_NATS_URL'] ?? 'nats://127.0.0.1:4222';
  final port = int.tryParse(environment['PODBUS_PLAIN_PORT'] ?? '') ?? 8088;
  final baseUri = Uri.parse('http://127.0.0.1:$port');
  final id = 'plain-dart-${DateTime.now().microsecondsSinceEpoch}';
  final client = HttpClient();

  await _waitForHttp(client, baseUri.resolve('/ready'), HttpStatus.ok);

  final queue = NatsJetStreamJobQueue(
    config: NatsMessagingConfig(
      servers: [Uri.parse(natsUrl)],
      connectTimeout: const Duration(seconds: 2),
      requestTimeout: const Duration(seconds: 5),
      jetStream: const NatsJetStreamConfig(
        enabled: true,
        streamName: _streamName,
        subjects: ['podbus.plain.jobs.>'],
        storage: NatsJetStreamStorage.memory,
      ),
    ),
  );

  try {
    await queue.connect();
    await queue.enqueue(
      _subject,
      {'id': id, 'source': 'plain-dart-probe'},
      idempotencyKey: id,
    );

    await _waitForHttp(
      client,
      baseUri.resolve('/processed/${Uri.encodeComponent(id)}'),
      HttpStatus.ok,
    );

    final shutdown = await client.postUrl(baseUri.resolve('/shutdown'));
    final shutdownResponse = await shutdown.close();
    await shutdownResponse.drain<void>();
    if (shutdownResponse.statusCode != HttpStatus.accepted) {
      throw StateError(
        'Plain Dart service rejected shutdown with '
        '${shutdownResponse.statusCode}.',
      );
    }

    stdout.writeln('Plain Dart service processed $id and shut down cleanly.');
  } finally {
    await queue.close(timeout: const Duration(seconds: 5));
    client.close(force: true);
  }
}

Future<void> _waitForHttp(
  HttpClient client,
  Uri uri,
  int expectedStatus,
) async {
  final deadline = DateTime.now().add(const Duration(seconds: 45));
  Object? lastError;

  while (DateTime.now().isBefore(deadline)) {
    try {
      final request = await client.getUrl(uri);
      final response = await request.close();
      await response.drain<void>();
      if (response.statusCode == expectedStatus) {
        return;
      }
      lastError = StateError(
        '$uri returned ${response.statusCode}; expected $expectedStatus.',
      );
    } on Object catch (error) {
      lastError = error;
    }
    await Future<void>.delayed(const Duration(milliseconds: 200));
  }

  throw TimeoutException('Timed out waiting for $uri: $lastError');
}
