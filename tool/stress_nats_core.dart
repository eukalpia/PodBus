import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:isolate';
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
  final subject = 'podbus.stress.nats.isolates.$runId';
  final queueGroup = 'podbus-stress-$runId';
  final events = ReceivePort();
  final consumerIsolates = <Isolate>[];
  final controlPorts = <int, SendPort>{};
  final workerResults = <int, _ConsumerResult>{};
  final ready = Completer<void>();
  final delivered = Completer<void>();
  final workersStopped = Completer<void>();
  var readyCount = 0;
  var reportedDeliveries = 0;
  var publisherFinished = false;
  Isolate? publisherIsolate;

  void completeError(Object error, [StackTrace? stackTrace]) {
    for (final completer in [ready, delivered, workersStopped]) {
      if (!completer.isCompleted) {
        completer.completeError(error, stackTrace ?? StackTrace.current);
      }
    }
  }

  void maybeCompleteDelivery() {
    if (publisherFinished &&
        reportedDeliveries >= messages &&
        !delivered.isCompleted) {
      delivered.complete();
    }
  }

  final eventSubscription = events.listen((dynamic message) {
    if (message is List && message.length >= 2) {
      completeError(
        StateError('NATS stress isolate failed: ${message.first}'),
        StackTrace.fromString(message[1].toString()),
      );
      return;
    }
    if (message is! Map<Object?, Object?>) {
      return;
    }

    switch (message['type']) {
      case 'ready':
        final id = message['id']! as int;
        controlPorts[id] = message['control']! as SendPort;
        readyCount += 1;
        if (readyCount == consumers && !ready.isCompleted) {
          ready.complete();
        }
      case 'progress':
        reportedDeliveries += message['delta']! as int;
        maybeCompleteDelivery();
      case 'publisher-done':
        publisherFinished = true;
        maybeCompleteDelivery();
      case 'result':
        final id = message['id']! as int;
        workerResults[id] = _ConsumerResult(
          delivered: message['delivered']! as int,
          localDuplicates: message['duplicates']! as int,
          bitmap: (message['bitmap']! as TransferableTypedData)
              .materialize()
              .asUint8List(),
        );
        if (workerResults.length == consumers && !workersStopped.isCompleted) {
          workersStopped.complete();
        }
      case 'error':
        completeError(
          StateError(
            'NATS stress ${message['role']} isolate failed: '
            '${message['error']}',
          ),
          StackTrace.fromString(message['stackTrace']?.toString() ?? ''),
        );
    }
  });

  stdout.writeln(
    'Starting NATS Core isolate stress: '
    '$messages messages, $producers publisher tasks, '
    '$consumers consumer isolates.',
  );

  final stopwatch = Stopwatch();
  try {
    for (var id = 0; id < consumers; id += 1) {
      consumerIsolates.add(
        await Isolate.spawn<Map<String, Object?>>(
          _consumerIsolateMain,
          {
            'events': events.sendPort,
            'id': id,
            'natsUrl': natsUrl,
            'subject': subject,
            'queueGroup': queueGroup,
            'messages': messages,
          },
          onError: events.sendPort,
          errorsAreFatal: true,
          debugName: 'podbus-nats-consumer-$id',
        ),
      );
    }

    await ready.future.timeout(const Duration(seconds: 30));
    stopwatch.start();

    publisherIsolate = await Isolate.spawn<Map<String, Object?>>(
      _publisherIsolateMain,
      {
        'events': events.sendPort,
        'natsUrl': natsUrl,
        'subject': subject,
        'messages': messages,
        'producers': producers,
        'payloadSize': payloadSize,
      },
      onError: events.sendPort,
      errorsAreFatal: true,
      debugName: 'podbus-nats-publisher',
    );

    await delivered.future.timeout(
      const Duration(minutes: 5),
      onTimeout: () {
        throw TimeoutException(
          'NATS Core reported $reportedDeliveries/$messages deliveries '
          'across $consumers consumer isolates.',
        );
      },
    );

    for (final control in controlPorts.values) {
      control.send('stop');
    }
    await workersStopped.future.timeout(const Duration(seconds: 30));
    stopwatch.stop();

    final union = Uint8List(messages);
    var unique = 0;
    var duplicates = 0;
    var totalDeliveries = 0;
    for (final result in workerResults.values) {
      totalDeliveries += result.delivered;
      duplicates += result.localDuplicates;
      for (var index = 0; index < messages; index += 1) {
        if (result.bitmap[index] == 0) {
          continue;
        }
        if (union[index] == 1) {
          duplicates += 1;
          continue;
        }
        union[index] = 1;
        unique += 1;
      }
    }

    if (unique != messages) {
      throw StateError(
        'NATS Core delivered $unique/$messages unique messages '
        '($totalDeliveries total deliveries, $duplicates duplicates).',
      );
    }

    final elapsedSeconds = stopwatch.elapsedMicroseconds / 1000000;
    final throughput = messages / elapsedSeconds;
    final evidence = <String, Object?>{
      'transport': 'nats-core',
      'broker': broker,
      'messages': messages,
      'received': totalDeliveries,
      'unique': unique,
      'duplicates': duplicates,
      'publisherTasks': producers,
      'consumerIsolates': consumers,
      'payloadBytes': payloadSize,
      'elapsedMs': stopwatch.elapsedMilliseconds,
      'throughputMessagesPerSecond': throughput,
      'delivery': 'at-most-once',
    };

    stdout.writeln();
    stdout.writeln(
      '| Transport | Mode | Messages | Unique | Elapsed | Throughput | Status |',
    );
    stdout.writeln('| --- | --- | ---: | ---: | ---: | ---: | --- |');
    stdout.writeln(
      '| NATS Core | isolated queue-group consumers | $messages | $unique | '
      '${stopwatch.elapsedMilliseconds} ms | '
      '${throughput.toStringAsFixed(1)} msg/s | completed |',
    );
    stdout.writeln();
    stdout.writeln('```json');
    stdout.writeln(const JsonEncoder.withIndent('  ').convert(evidence));
    stdout.writeln('```');
  } finally {
    for (final control in controlPorts.values) {
      control.send('stop');
    }
    publisherIsolate?.kill(priority: Isolate.immediate);
    for (final isolate in consumerIsolates) {
      isolate.kill(priority: Isolate.immediate);
    }
    await eventSubscription.cancel();
    events.close();
  }
}

Future<void> _publisherIsolateMain(Map<String, Object?> arguments) async {
  final events = arguments['events']! as SendPort;
  final natsUrl = arguments['natsUrl']! as String;
  final subject = arguments['subject']! as String;
  final messages = arguments['messages']! as int;
  final producers = arguments['producers']! as int;
  final payloadSize = arguments['payloadSize']! as int;
  final bus = NatsMessageBus(config: _natsConfig(natsUrl));

  try {
    await bus.connect();
    var nextIndex = 0;

    Future<void> publishWorker() async {
      while (true) {
        final index = nextIndex;
        nextIndex += 1;
        if (index >= messages) {
          return;
        }
        await bus.publish(subject, _payload(index, payloadSize));
      }
    }

    await Future.wait(List.generate(producers, (_) => publishWorker()));
    final health = await bus.healthCheck();
    if (health.status == HealthStatus.unhealthy) {
      throw StateError('NATS publisher flush failed: ${health.message}');
    }
    events.send({'type': 'publisher-done'});
  } on Object catch (error, stackTrace) {
    events.send({
      'type': 'error',
      'role': 'publisher',
      'error': error.toString(),
      'stackTrace': stackTrace.toString(),
    });
  } finally {
    await _closeQuietly(() => bus.close(timeout: const Duration(seconds: 5)));
  }
}

Future<void> _consumerIsolateMain(Map<String, Object?> arguments) async {
  final events = arguments['events']! as SendPort;
  final id = arguments['id']! as int;
  final natsUrl = arguments['natsUrl']! as String;
  final subject = arguments['subject']! as String;
  final queueGroup = arguments['queueGroup']! as String;
  final messages = arguments['messages']! as int;
  final control = ReceivePort();
  final stopped = Completer<void>();
  final bitmap = Uint8List(messages);
  final bus = NatsMessageBus(config: _natsConfig(natsUrl));
  Subscription? subscription;
  Timer? progressTimer;
  var delivered = 0;
  var duplicates = 0;
  var unreported = 0;

  void flushProgress() {
    if (unreported == 0) {
      return;
    }
    events.send({'type': 'progress', 'id': id, 'delta': unreported});
    unreported = 0;
  }

  final controlSubscription = control.listen((dynamic command) {
    if (command == 'stop' && !stopped.isCompleted) {
      stopped.complete();
    }
  });

  try {
    await bus.connect();
    subscription = await bus.subscribe<Map<String, Object?>>(
      subject,
      queueGroup: queueGroup,
      handler: (_, payload) async {
        final index = payload['index'];
        if (index is! int || index < 0 || index >= messages) {
          throw StateError('Received invalid stress index: $index');
        }
        delivered += 1;
        unreported += 1;
        if (bitmap[index] == 1) {
          duplicates += 1;
        } else {
          bitmap[index] = 1;
        }
      },
    );
    final health = await bus.healthCheck();
    if (health.status == HealthStatus.unhealthy) {
      throw StateError('NATS consumer $id flush failed: ${health.message}');
    }

    progressTimer = Timer.periodic(
      const Duration(milliseconds: 100),
      (_) => flushProgress(),
    );
    events.send({'type': 'ready', 'id': id, 'control': control.sendPort});
    await stopped.future;
    flushProgress();
    await subscription.close();
    await bus.close(timeout: const Duration(seconds: 5));
    events.send({
      'type': 'result',
      'id': id,
      'delivered': delivered,
      'duplicates': duplicates,
      'bitmap': TransferableTypedData.fromList([bitmap]),
    });
  } on Object catch (error, stackTrace) {
    events.send({
      'type': 'error',
      'role': 'consumer-$id',
      'error': error.toString(),
      'stackTrace': stackTrace.toString(),
    });
  } finally {
    progressTimer?.cancel();
    await controlSubscription.cancel();
    control.close();
    if (subscription != null) {
      await _closeQuietly(subscription.close);
    }
    await _closeQuietly(() => bus.close(timeout: const Duration(seconds: 5)));
  }
}

NatsMessagingConfig _natsConfig(String url) {
  return NatsMessagingConfig(
    servers: [Uri.parse(url)],
    connectTimeout: const Duration(seconds: 2),
    requestTimeout: const Duration(seconds: 10),
  );
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

final class _ConsumerResult {
  const _ConsumerResult({
    required this.delivered,
    required this.localDuplicates,
    required this.bitmap,
  });

  final int delivered;
  final int localDuplicates;
  final Uint8List bitmap;
}
