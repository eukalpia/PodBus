import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_nats/podbus_nats.dart';

final _paddingCache = <int, String>{0: ''};

Future<void> main() async {
  final messages = _positiveInt('PODBUS_STRESS_MESSAGES', 1000000);
  final producers = _positiveInt('PODBUS_STRESS_PRODUCERS', 32);
  final consumers = _positiveInt('PODBUS_STRESS_CONSUMERS', 16);
  final payloadSize = _positiveInt('PODBUS_STRESS_PAYLOAD_SIZES', 256);
  final broker = Platform.environment['PODBUS_STRESS_BROKER'] ?? 'local';
  final natsUrl =
      Platform.environment['PODBUS_NATS_URL'] ?? 'nats://127.0.0.1:4222';
  final runId = DateTime.now().microsecondsSinceEpoch;
  final subject = 'podbus.stress.nats.multi_connection.$runId';
  final queueGroup = 'podbus-stress-$runId';
  final config = NatsMessagingConfig(
    servers: [Uri.parse(natsUrl)],
    connectTimeout: const Duration(seconds: 2),
    requestTimeout: const Duration(seconds: 10),
  );

  final publisher = NatsMessageBus(config: config);
  final subscribers = List<NatsMessageBus>.generate(
    consumers,
    (_) => NatsMessageBus(config: config),
  );
  final subscriptions = <Subscription>[];
  final seen = Uint8List(messages);
  final complete = Completer<void>();
  var received = 0;
  var duplicates = 0;

  stdout.writeln(
    'Starting NATS Core multi-connection stress: '
    '$messages messages, $producers producers, $consumers consumer sockets.',
  );

  try {
    await Future.wait([
      publisher.connect(),
      for (final subscriber in subscribers) subscriber.connect(),
    ]);

    for (final subscriber in subscribers) {
      subscriptions.add(
        await subscriber.subscribe<Map<String, Object?>>(
          subject,
          queueGroup: queueGroup,
          handler: (_, payload) async {
            final index = payload['index'];
            if (index is! int || index < 0 || index >= messages) {
              throw StateError('Received invalid stress index: $index');
            }
            if (seen[index] == 1) {
              duplicates += 1;
              return;
            }
            seen[index] = 1;
            received += 1;
            if (received == messages && !complete.isCompleted) {
              complete.complete();
            }
          },
        ),
      );
    }

    final readiness = await Future.wait([
      for (final subscriber in subscribers) subscriber.healthCheck(),
    ]);
    final unhealthy = readiness.where(
      (result) => result.status == HealthStatus.unhealthy,
    );
    if (unhealthy.isNotEmpty) {
      throw StateError(
        'NATS subscriptions did not become ready: '
        '${unhealthy.map((result) => result.message).join('; ')}',
      );
    }

    final stopwatch = Stopwatch()..start();
    var nextIndex = 0;

    Future<void> publishWorker() async {
      while (true) {
        final index = nextIndex;
        nextIndex += 1;
        if (index >= messages) {
          return;
        }
        await publisher.publish(subject, _payload(index, payloadSize));
      }
    }

    await Future.wait(List.generate(producers, (_) => publishWorker()));
    await complete.future.timeout(
      const Duration(minutes: 5),
      onTimeout: () {
        throw TimeoutException(
          'NATS Core delivered $received/$messages unique messages '
          'across $consumers consumer connections.',
        );
      },
    );
    stopwatch.stop();

    final elapsedSeconds = stopwatch.elapsedMicroseconds / 1000000;
    final throughput = messages / elapsedSeconds;
    final evidence = <String, Object?>{
      'transport': 'nats-core',
      'broker': broker,
      'messages': messages,
      'received': received,
      'duplicates': duplicates,
      'producers': producers,
      'consumerConnections': consumers,
      'payloadBytes': payloadSize,
      'elapsedMs': stopwatch.elapsedMilliseconds,
      'throughputMessagesPerSecond': throughput,
      'delivery': 'at-most-once',
    };

    stdout.writeln();
    stdout.writeln(
      '| Transport | Mode | Messages | Received | Elapsed | Throughput | Status |',
    );
    stdout.writeln('| --- | --- | ---: | ---: | ---: | ---: | --- |');
    stdout.writeln(
      '| NATS Core | multi-connection queue group | $messages | $received | '
      '${stopwatch.elapsedMilliseconds} ms | '
      '${throughput.toStringAsFixed(1)} msg/s | completed |',
    );
    stdout.writeln();
    stdout.writeln('```json');
    stdout.writeln(const JsonEncoder.withIndent('  ').convert(evidence));
    stdout.writeln('```');
  } finally {
    for (final subscription in subscriptions.reversed) {
      await _closeQuietly(subscription.close);
    }
    await _closeQuietly(
      () => publisher.close(timeout: const Duration(seconds: 5)),
    );
    for (final subscriber in subscribers) {
      await _closeQuietly(
        () => subscriber.close(timeout: const Duration(seconds: 5)),
      );
    }
  }
}

Map<String, Object?> _payload(int index, int payloadSize) {
  final prefix = '$index:';
  final paddingLength = payloadSize - prefix.length;
  return {'index': index, 'payload': '$prefix${_padding(paddingLength)}'};
}

String _padding(int length) {
  if (length <= 0) {
    return '';
  }
  return _paddingCache.putIfAbsent(length, () {
    final bytes = Uint8List(length)..fillRange(0, length, 0x78);
    return String.fromCharCodes(bytes);
  });
}

Future<void> _closeQuietly(Future<void> Function() close) async {
  try {
    await close();
  } on Object {
    // Cleanup must not hide the stress result.
  }
}

int _positiveInt(String name, int fallback) {
  final raw = Platform.environment[name];
  if (raw == null || raw.trim().isEmpty) {
    return fallback;
  }
  final first = raw.split(',').first.trim();
  final value = int.tryParse(first);
  if (value == null || value < 1) {
    throw ArgumentError.value(raw, name, 'Expected a positive integer.');
  }
  return value;
}
