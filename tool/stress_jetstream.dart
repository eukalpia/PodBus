import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_nats/podbus_nats.dart';

final _paddingCache = <int, String>{0: ''};

Future<void> main() async {
  final messages = _positiveInt('PODBUS_STRESS_MESSAGES', 250000);
  final producers = _positiveInt('PODBUS_STRESS_PRODUCERS', 64);
  final consumers = _positiveInt('PODBUS_STRESS_CONSUMERS', 8);
  final payloadSize = _positiveInt('PODBUS_STRESS_PAYLOAD_SIZES', 256);
  final fetchBatch = _positiveInt('PODBUS_JETSTREAM_FETCH_BATCH_SIZE', 128);
  final mode = _firstValue('PODBUS_STRESS_MODES', 'durable');
  final storage = mode == 'worker'
      ? NatsJetStreamStorage.file
      : NatsJetStreamStorage.memory;
  final broker = Platform.environment['PODBUS_STRESS_BROKER'] ?? 'local';
  final natsUrl =
      Platform.environment['PODBUS_NATS_URL'] ?? 'nats://127.0.0.1:4222';
  final runId = DateTime.now().microsecondsSinceEpoch.toString();
  final stream = 'PODBUS_STRESS_$runId';
  final subject = 'podbus.stress.jetstream.$runId';
  const durableName = 'stress_workers';

  final events = ReceivePort();
  final consumerIsolates = <Isolate>[];
  final controlPorts = <int, SendPort>{};
  final results = <int, _ConsumerResult>{};
  final readyById = <int, Completer<void>>{};
  final delivered = Completer<void>();
  final stopped = Completer<void>();
  var reportedDeliveries = 0;
  var publisherFinished = false;
  Isolate? publisherIsolate;

  void failAll(Object error, [StackTrace? stackTrace]) {
    final trace = stackTrace ?? StackTrace.current;
    for (final completer in [...readyById.values, delivered, stopped]) {
      if (!completer.isCompleted) {
        completer.completeError(error, trace);
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

  final subscription = events.listen((dynamic message) {
    if (message is List && message.length >= 2) {
      failAll(
        StateError('JetStream stress isolate failed: ${message.first}'),
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
        final completer = readyById[id];
        if (completer != null && !completer.isCompleted) {
          completer.complete();
        }
      case 'progress':
        reportedDeliveries += message['delta']! as int;
        maybeCompleteDelivery();
      case 'publisher-done':
        publisherFinished = true;
        maybeCompleteDelivery();
      case 'result':
        final id = message['id']! as int;
        results[id] = _ConsumerResult(
          delivered: message['delivered']! as int,
          localDuplicates: message['duplicates']! as int,
          bitmap: (message['bitmap']! as TransferableTypedData)
              .materialize()
              .asUint8List(),
        );
        if (results.length == consumers && !stopped.isCompleted) {
          stopped.complete();
        }
      case 'error':
        failAll(
          StateError(
            'JetStream stress ${message['role']} isolate failed: '
            '${message['error']}',
          ),
          StackTrace.fromString(message['stackTrace']?.toString() ?? ''),
        );
    }
  });

  stdout.writeln(
    'Starting JetStream isolate stress: $messages messages, '
    '$producers publisher tasks, $consumers consumer isolates, '
    '${storage.name} storage.',
  );

  final stopwatch = Stopwatch();
  try {
    // Start the first consumer alone so it creates the durable pull consumer.
    // Additional isolates then attach to the same durable without a creation race.
    for (var id = 0; id < consumers; id += 1) {
      readyById[id] = Completer<void>();
      consumerIsolates.add(
        await Isolate.spawn<Map<String, Object?>>(
          _consumerMain,
          {
            'events': events.sendPort,
            'id': id,
            'natsUrl': natsUrl,
            'stream': stream,
            'subject': subject,
            'durableName': durableName,
            'messages': messages,
            'storage': storage.name,
            'fetchBatch': fetchBatch,
          },
          onError: events.sendPort,
          errorsAreFatal: true,
          debugName: 'podbus-jetstream-consumer-$id',
        ),
      );
      await readyById[id]!.future.timeout(const Duration(seconds: 30));
    }

    stopwatch.start();
    publisherIsolate = await Isolate.spawn<Map<String, Object?>>(
      _publisherMain,
      {
        'events': events.sendPort,
        'natsUrl': natsUrl,
        'stream': stream,
        'subject': subject,
        'messages': messages,
        'producers': producers,
        'payloadSize': payloadSize,
        'storage': storage.name,
        'fetchBatch': fetchBatch,
      },
      onError: events.sendPort,
      errorsAreFatal: true,
      debugName: 'podbus-jetstream-publisher',
    );

    await delivered.future.timeout(
      const Duration(minutes: 30),
      onTimeout: () {
        throw TimeoutException(
          'JetStream reported $reportedDeliveries/$messages deliveries '
          'across $consumers consumer isolates.',
        );
      },
    );

    for (final control in controlPorts.values) {
      control.send('stop');
    }
    await stopped.future.timeout(const Duration(seconds: 60));
    stopwatch.stop();

    final union = Uint8List(messages);
    var unique = 0;
    var duplicates = 0;
    var totalDeliveries = 0;
    for (final result in results.values) {
      totalDeliveries += result.delivered;
      duplicates += result.localDuplicates;
      for (var index = 0; index < messages; index += 1) {
        if (result.bitmap[index] == 0) {
          continue;
        }
        if (union[index] == 1) {
          duplicates += 1;
        } else {
          union[index] = 1;
          unique += 1;
        }
      }
    }

    if (unique != messages) {
      throw StateError(
        'JetStream delivered $unique/$messages unique messages '
        '($totalDeliveries total, $duplicates duplicates).',
      );
    }

    final elapsedSeconds = stopwatch.elapsedMicroseconds / 1000000;
    final throughput = messages / elapsedSeconds;
    final evidence = <String, Object?>{
      'transport': 'nats-jetstream',
      'mode': mode,
      'broker': broker,
      'messages': messages,
      'received': totalDeliveries,
      'unique': unique,
      'duplicates': duplicates,
      'publisherTasks': producers,
      'consumerIsolates': consumers,
      'fetchBatch': fetchBatch,
      'payloadBytes': payloadSize,
      'storage': storage.name,
      'elapsedMs': stopwatch.elapsedMilliseconds,
      'throughputMessagesPerSecond': throughput,
      'publisherConfirmation': 'JetStream PubAck',
      'ackMode': 'manual',
    };

    stdout.writeln();
    stdout.writeln(
      '| Transport | Mode | Messages | Unique | Elapsed | Throughput | Status |',
    );
    stdout.writeln('| --- | --- | ---: | ---: | ---: | ---: | --- |');
    stdout.writeln(
      '| JetStream | $mode / ${storage.name} / isolated consumers | '
      '$messages | $unique | ${stopwatch.elapsedMilliseconds} ms | '
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
    await subscription.cancel();
    events.close();
  }
}

Future<void> _publisherMain(Map<String, Object?> arguments) async {
  final events = arguments['events']! as SendPort;
  final natsUrl = arguments['natsUrl']! as String;
  final stream = arguments['stream']! as String;
  final subject = arguments['subject']! as String;
  final messages = arguments['messages']! as int;
  final producers = arguments['producers']! as int;
  final payloadSize = arguments['payloadSize']! as int;
  final storage = _storage(arguments['storage']! as String);
  final fetchBatch = arguments['fetchBatch']! as int;
  final queue = _queue(
    natsUrl: natsUrl,
    stream: stream,
    subject: subject,
    storage: storage,
    fetchBatch: fetchBatch,
  );

  try {
    await queue.connect();
    var nextIndex = 0;

    Future<void> publishWorker() async {
      while (true) {
        final index = nextIndex;
        nextIndex += 1;
        if (index >= messages) {
          return;
        }
        await queue.enqueue(subject, _payload(index, payloadSize));
      }
    }

    await Future.wait(List.generate(producers, (_) => publishWorker()));
    final health = await queue.healthCheck();
    if (health.status == HealthStatus.unhealthy) {
      throw StateError('JetStream publisher flush failed: ${health.message}');
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
    await _closeQuietly(() => queue.close(timeout: const Duration(seconds: 10)));
  }
}

Future<void> _consumerMain(Map<String, Object?> arguments) async {
  final events = arguments['events']! as SendPort;
  final id = arguments['id']! as int;
  final natsUrl = arguments['natsUrl']! as String;
  final stream = arguments['stream']! as String;
  final subject = arguments['subject']! as String;
  final durableName = arguments['durableName']! as String;
  final messages = arguments['messages']! as int;
  final storage = _storage(arguments['storage']! as String);
  final fetchBatch = arguments['fetchBatch']! as int;
  final control = ReceivePort();
  final stop = Completer<void>();
  final bitmap = Uint8List(messages);
  final queue = _queue(
    natsUrl: natsUrl,
    stream: stream,
    subject: subject,
    storage: storage,
    fetchBatch: fetchBatch,
  );
  Worker? worker;
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
    if (command == 'stop' && !stop.isCompleted) {
      stop.complete();
    }
  });

  try {
    await queue.connect();
    worker = await queue.worker<Map<String, Object?>>(
      subject,
      durableName: durableName,
      concurrency: 1,
      handler: (_, payload) async {
        final index = payload['index'];
        if (index is! int || index < 0 || index >= messages) {
          throw StateError('Received invalid JetStream index: $index');
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
    final health = await queue.healthCheck();
    if (health.status == HealthStatus.unhealthy) {
      throw StateError('JetStream consumer $id is unhealthy: ${health.message}');
    }
    progressTimer = Timer.periodic(
      const Duration(milliseconds: 100),
      (_) => flushProgress(),
    );
    events.send({'type': 'ready', 'id': id, 'control': control.sendPort});
    await stop.future;
    flushProgress();
    await worker.close();
    await queue.close(timeout: const Duration(seconds: 10));
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
    if (worker != null) {
      await _closeQuietly(worker.close);
    }
    await _closeQuietly(() => queue.close(timeout: const Duration(seconds: 10)));
  }
}

NatsJetStreamJobQueue _queue({
  required String natsUrl,
  required String stream,
  required String subject,
  required NatsJetStreamStorage storage,
  required int fetchBatch,
}) {
  return NatsJetStreamJobQueue(
    config: NatsMessagingConfig(
      servers: [Uri.parse(natsUrl)],
      connectTimeout: const Duration(seconds: 2),
      requestTimeout: const Duration(seconds: 10),
      jetStream: NatsJetStreamConfig(
        enabled: true,
        streamName: stream,
        subjects: [subject],
        storage: storage,
        consumerConfig: const NatsJetStreamConsumerConfig(
          ackWait: Duration(seconds: 30),
          maxDeliver: 10,
          maxAckPending: 4096,
        ),
      ),
    ),
    fetchTimeout: const Duration(milliseconds: 200),
    fetchBatchSize: fetchBatch,
  );
}

NatsJetStreamStorage _storage(String value) {
  return value == NatsJetStreamStorage.file.name
      ? NatsJetStreamStorage.file
      : NatsJetStreamStorage.memory;
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

String _firstValue(String name, String fallback) {
  final raw = Platform.environment[name];
  if (raw == null || raw.trim().isEmpty) {
    return fallback;
  }
  return raw.split(',').first.trim();
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
