import 'dart:async';
import 'dart:io';
import 'dart:math' as math;

import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_kafka/podbus_kafka.dart';
import 'package:podbus_nats/podbus_nats.dart';
import 'package:podbus_rabbitmq/podbus_rabbitmq.dart';

enum StressMode {
  fast,
  durable,
  worker,
  failure;

  static StressMode parse(String value) {
    return switch (value.trim().toLowerCase()) {
      'fast' => StressMode.fast,
      'durable' => StressMode.durable,
      'worker' => StressMode.worker,
      'failure' => StressMode.failure,
      _ => throw ArgumentError('Unknown stress mode: $value'),
    };
  }
}

final class StressOptions {
  const StressOptions({
    required this.messages,
    required this.failureMessages,
    required this.modes,
    required this.transports,
    required this.payloadSizes,
    required this.consumerCounts,
    required this.producerCounts,
    required this.handlerSleeps,
    required this.broker,
    required this.machine,
  });

  factory StressOptions.fromEnvironment(Map<String, String> environment) {
    final messages = _envInt(environment, 'PODBUS_STRESS_MESSAGES', 2000);
    final legacyConcurrency = _envInt(
      environment,
      'PODBUS_STRESS_CONCURRENCY',
      100,
    );
    final modes = _envEnumList(
      environment,
      'PODBUS_STRESS_MODES',
      StressMode.parse,
      StressMode.values,
      singleValueKey: 'PODBUS_STRESS_MODE',
    );

    return StressOptions(
      messages: messages,
      failureMessages: _envInt(
        environment,
        'PODBUS_STRESS_FAILURE_MESSAGES',
        math.min(messages, 100),
      ),
      modes: modes,
      transports: _envList(environment, 'PODBUS_STRESS_TRANSPORTS', [
        'nats',
        'jetstream',
        'rabbitmq',
        'kafka',
      ]),
      payloadSizes: _envIntList(environment, 'PODBUS_STRESS_PAYLOAD_SIZES', [
        256,
      ]),
      consumerCounts: _envIntList(environment, 'PODBUS_STRESS_CONSUMERS', [1]),
      producerCounts: _envIntList(environment, 'PODBUS_STRESS_PRODUCERS', [
        legacyConcurrency,
      ]),
      handlerSleeps: [
        for (final ms in _envIntList(
          environment,
          'PODBUS_STRESS_HANDLER_SLEEP_MS',
          [0],
        ))
          Duration(milliseconds: ms),
      ],
      broker: environment['PODBUS_STRESS_BROKER'] ?? 'Docker/local',
      machine: _machineSummary(),
    );
  }

  final int messages;
  final int failureMessages;
  final List<StressMode> modes;
  final List<String> transports;
  final List<int> payloadSizes;
  final List<int> consumerCounts;
  final List<int> producerCounts;
  final List<Duration> handlerSleeps;
  final String broker;
  final String machine;

  Iterable<StressScenario> scenarios() sync* {
    for (final mode in modes) {
      final modeMessages = mode == StressMode.failure
          ? failureMessages
          : messages;
      final modePayloadSizes = mode == StressMode.failure
          ? payloadSizes.take(1)
          : payloadSizes;
      final modeConsumerCounts = mode == StressMode.failure
          ? consumerCounts.take(1)
          : consumerCounts;
      final modeProducerCounts = mode == StressMode.failure
          ? producerCounts.take(1)
          : producerCounts;
      final modeSleeps = mode == StressMode.worker
          ? handlerSleeps
          : const [Duration.zero];

      for (final transport in transports) {
        for (final payloadSize in modePayloadSizes) {
          for (final producers in modeProducerCounts) {
            for (final consumers in modeConsumerCounts) {
              for (final sleep in modeSleeps) {
                yield StressScenario(
                  mode: mode,
                  transport: transport,
                  messages: modeMessages,
                  payloadSize: payloadSize,
                  producers: producers,
                  consumers: consumers,
                  handlerSleep: sleep,
                );
              }
            }
          }
        }
      }
    }
  }

  String formatParameterTable() {
    return [
      'Parameters',
      'messages: $messages',
      'failureMessages: $failureMessages',
      'modes: ${modes.map((mode) => mode.name).join(',')}',
      'transports: ${transports.join(',')}',
      'payloadSizes: ${payloadSizes.join(',')} bytes',
      'consumers: ${consumerCounts.join(',')}',
      'producers: ${producerCounts.join(',')}',
      'handlerSleepMs: '
          '${handlerSleeps.map((value) => value.inMilliseconds).join(',')}',
      'broker: $broker',
      'machine: $machine',
    ].join('\n');
  }
}

final class StressScenario {
  const StressScenario({
    required this.mode,
    required this.transport,
    required this.messages,
    required this.payloadSize,
    required this.producers,
    required this.consumers,
    required this.handlerSleep,
  });

  final StressMode mode;
  final String transport;
  final int messages;
  final int payloadSize;
  final int producers;
  final int consumers;
  final Duration handlerSleep;
}

final class BrokerEndpoint {
  const BrokerEndpoint(this.host, this.port);

  final String host;
  final int port;

  @override
  bool operator ==(Object other) {
    return other is BrokerEndpoint && other.host == host && other.port == port;
  }

  @override
  int get hashCode => Object.hash(host, port);
}

Future<void> main() async {
  final options = StressOptions.fromEnvironment(Platform.environment);
  stdout.writeln(options.formatParameterTable());
  stdout.writeln('');
  stdout.writeln(StressResult.markdownHeader);
  stdout.writeln(StressResult.markdownSeparator);

  final results = <StressResult>[];
  for (final scenario in options.scenarios()) {
    final result = await _runScenario(options, scenario);
    results.add(result);
    stdout.writeln(result.formatMarkdownRow());
  }

  stdout.writeln('\nSummary');
  for (final result in results) {
    stdout.writeln(result.formatCompact());
  }
}

Future<StressResult> _runScenario(
  StressOptions options,
  StressScenario scenario,
) async {
  try {
    final unsupportedReason = _staticUnsupportedReason(scenario);
    if (unsupportedReason != null) {
      return StressResult.skipped(
        scenario: scenario,
        broker: options.broker,
        machine: options.machine,
        reason: unsupportedReason,
      );
    }

    final brokerUnavailableReason = await _brokerUnavailableReason(
      scenario.transport,
      Platform.environment,
    );
    if (brokerUnavailableReason != null) {
      return StressResult.skipped(
        scenario: scenario,
        broker: options.broker,
        machine: options.machine,
        reason: brokerUnavailableReason,
      );
    }

    return switch (scenario.mode) {
      StressMode.fast => await _runFastMode(options, scenario),
      StressMode.durable => await _runDurableMode(options, scenario),
      StressMode.worker => await _runWorkerMode(options, scenario),
      StressMode.failure => await _runFailureMode(options, scenario),
    };
  } on Object catch (error, stackTrace) {
    return StressResult.failed(
      scenario: scenario,
      broker: options.broker,
      machine: options.machine,
      error: error,
      stackTrace: stackTrace,
    );
  }
}

Future<StressResult> _runFastMode(
  StressOptions options,
  StressScenario scenario,
) {
  return switch (scenario.transport) {
    'nats' => _stressNatsCoreEvents(options, scenario),
    'rabbitmq' => _stressRabbitMqEvents(options, scenario, durable: false),
    'kafka' => _stressKafkaEvents(options, scenario),
    'jetstream' => _unsupported(
      options,
      scenario,
      'JetStream is a durable mode transport, not a fast fire-and-forget mode.',
    ),
    _ => _unsupported(options, scenario, 'Unknown transport.'),
  };
}

String? _staticUnsupportedReason(StressScenario scenario) {
  return switch ((scenario.mode, scenario.transport)) {
    (StressMode.fast, 'jetstream') =>
      'JetStream is a durable mode transport, not a fast fire-and-forget mode.',
    (StressMode.durable, 'nats') =>
      'NATS Core has no durable storage or replay semantics.',
    (StressMode.worker, 'nats') =>
      'NATS Core is pub/sub only; use JetStream for worker mode.',
    (StressMode.worker, 'kafka') when scenario.consumers != 1 =>
      'Kafka worker concurrency is limited to 1 in this adapter.',
    (StressMode.failure, 'nats') =>
      'NATS Core has no broker-side ack, redelivery, or dead-letter semantics.',
    (StressMode.failure, 'kafka') =>
      'Kafka failure mode is disabled until DLQ publish, delivery flush, and offset commit semantics are fixed in the adapter.',
    (_, 'nats' || 'jetstream' || 'rabbitmq' || 'kafka') => null,
    _ => 'Unknown transport.',
  };
}

Future<StressResult> _runDurableMode(
  StressOptions options,
  StressScenario scenario,
) {
  return switch (scenario.transport) {
    'jetstream' => _stressNatsJetStreamJobs(
      options,
      scenario,
      storage: NatsJetStreamStorage.file,
      modeNote: 'file storage, explicit ack, JetStream publish ack',
    ),
    'rabbitmq' => _stressRabbitMqJobs(
      options,
      scenario,
      durable: true,
      modeNote: 'persistent messages; publisher confirms not implemented',
    ),
    'kafka' => _stressKafkaJobs(
      options,
      scenario,
      modeNote: 'manual offset commit; acks and partitions not configurable',
    ),
    'nats' => _unsupported(
      options,
      scenario,
      'NATS Core has no durable storage or replay semantics.',
    ),
    _ => _unsupported(options, scenario, 'Unknown transport.'),
  };
}

Future<StressResult> _runWorkerMode(
  StressOptions options,
  StressScenario scenario,
) {
  return switch (scenario.transport) {
    'jetstream' => _stressNatsJetStreamJobs(
      options,
      scenario,
      storage: NatsJetStreamStorage.file,
      modeNote: 'manual ack worker',
    ),
    'rabbitmq' => _stressRabbitMqJobs(
      options,
      scenario,
      durable: true,
      modeNote: 'manual ack worker; delayed retry uses client-side sleep',
    ),
    'kafka' =>
      scenario.consumers == 1
          ? _stressKafkaJobs(
              options,
              scenario,
              modeNote:
                  'single consumer worker; partition parallelism not tuned',
            )
          : _unsupported(
              options,
              scenario,
              'Kafka worker concurrency is limited to 1 in this adapter.',
            ),
    'nats' => _unsupported(
      options,
      scenario,
      'NATS Core is pub/sub only; use JetStream for worker mode.',
    ),
    _ => _unsupported(options, scenario, 'Unknown transport.'),
  };
}

Future<StressResult> _runFailureMode(
  StressOptions options,
  StressScenario scenario,
) {
  return switch (scenario.transport) {
    'jetstream' => _stressNatsJetStreamDeadLetter(options, scenario),
    'rabbitmq' => _stressRabbitMqDeadLetter(options, scenario),
    'kafka' => _stressKafkaDeadLetter(options, scenario),
    'nats' => _unsupported(
      options,
      scenario,
      'NATS Core has no broker-side ack, redelivery, or dead-letter semantics.',
    ),
    _ => _unsupported(options, scenario, 'Unknown transport.'),
  };
}

Future<StressResult> _stressNatsCoreEvents(
  StressOptions options,
  StressScenario scenario,
) async {
  final id = _runId();
  final subject = 'podbus.stress.nats.$id';
  final bus = NatsMessageBus(
    config: NatsMessagingConfig(
      servers: [Uri.parse(_env('PODBUS_NATS_URL', 'nats://localhost:4222'))],
      connectTimeout: const Duration(seconds: 2),
      requestTimeout: const Duration(seconds: 10),
    ),
  );

  await bus.connect();
  try {
    final received = _Counter(scenario.messages);
    for (var i = 0; i < scenario.consumers; i += 1) {
      await bus.subscribe<Map<String, Object?>>(
        subject,
        queueGroup: 'stress',
        handler: (_, payload) async {
          received.add(payload['index'] as int);
        },
      );
    }

    final elapsed = await _time(() async {
      await _publishWindowed(
        scenario.messages,
        scenario.producers,
        (index) => bus.publish(subject, _payload(index, scenario.payloadSize)),
      );
      await received.done.timeout(_eventTimeout);
    });

    return StressResult.completed(
      scenario: scenario,
      broker: options.broker,
      machine: options.machine,
      received: received.count,
      elapsed: elapsed,
      ackMode: 'none',
      durability: 'no',
      publisherConfirms: 'no',
      notes: 'at-most-once Core pub/sub',
    );
  } finally {
    await bus.close(timeout: const Duration(seconds: 5));
  }
}

Future<StressResult> _stressRabbitMqEvents(
  StressOptions options,
  StressScenario scenario, {
  required bool durable,
}) async {
  final id = _runId();
  final subject = 'podbus.stress.rabbitmq.$id';
  final bus = RabbitMqMessageBus(
    config: RabbitMqMessagingConfig(
      uri: Uri.parse(
        _env('PODBUS_RABBITMQ_URL', 'amqp://guest:guest@localhost:5672'),
      ),
      exchange: 'podbus.stress.events.$id',
      deadLetterExchange: 'podbus.stress.dead.$id',
      durable: durable,
      prefetchCount: math.max(1, scenario.consumers),
    ),
  );

  await bus.connect();
  try {
    final received = _Counter(scenario.messages);
    for (var i = 0; i < scenario.consumers; i += 1) {
      await bus.subscribe<Map<String, Object?>>(
        subject,
        queueGroup: 'stress',
        handler: (_, payload) async {
          received.add(payload['index'] as int);
        },
      );
    }

    final elapsed = await _time(() async {
      await _publishWindowed(
        scenario.messages,
        scenario.producers,
        (index) => bus.publish(subject, _payload(index, scenario.payloadSize)),
      );
      await received.done.timeout(_eventTimeout);
    });

    return StressResult.completed(
      scenario: scenario,
      broker: options.broker,
      machine: options.machine,
      received: received.count,
      elapsed: elapsed,
      ackMode: 'manual',
      durability: durable ? 'persistent queue/message' : 'no',
      publisherConfirms: 'no',
      notes: durable ? 'confirms not implemented' : 'non-persistent fast path',
    );
  } finally {
    await bus.close(timeout: const Duration(seconds: 5));
  }
}

Future<StressResult> _stressKafkaEvents(
  StressOptions options,
  StressScenario scenario,
) async {
  final id = _runId();
  final topic = 'podbus.stress.kafka.$id';
  final bus = KafkaEventBus(
    config: KafkaMessagingConfig(
      brokers: [_env('PODBUS_KAFKA_BROKER', 'localhost:9092')],
      clientId: 'podbus-stress-$id',
      groupId: 'podbus-stress-$id',
      requestTimeout: const Duration(seconds: 1),
    ),
  );

  await bus.connect();
  try {
    final received = _Counter(scenario.messages);
    for (var i = 0; i < scenario.consumers; i += 1) {
      await bus.subscribe<Map<String, Object?>>(
        topic,
        queueGroup: 'podbus-stress-$id',
        handler: (_, payload) async {
          received.add(payload['index'] as int);
        },
      );
    }

    final elapsed = await _time(() async {
      await _publishWindowed(
        scenario.messages,
        scenario.producers,
        (index) => bus.publish(topic, _payload(index, scenario.payloadSize)),
      );
      await bus.healthCheck();
      await received.done.timeout(_eventTimeout);
    });

    return StressResult.completed(
      scenario: scenario,
      broker: options.broker,
      machine: options.machine,
      received: received.count,
      elapsed: elapsed,
      ackMode: 'commit',
      durability: 'event log, untuned',
      publisherConfirms: 'not configurable',
      notes: 'partitions, batching, linger, and acks are not configured',
    );
  } finally {
    await bus.close(timeout: const Duration(seconds: 5));
  }
}

Future<StressResult> _stressNatsJetStreamJobs(
  StressOptions options,
  StressScenario scenario, {
  required NatsJetStreamStorage storage,
  required String modeNote,
}) async {
  final id = _runId();
  final topic = 'podbus.stress.jetstream.$id';
  final queue = _natsJetStreamQueue(id, [topic], storage);

  await queue.connect();
  try {
    await _enqueueJobs(
      scenario.messages,
      scenario.producers,
      (index) => queue.enqueue(topic, _payload(index, scenario.payloadSize)),
    );

    final received = _Counter(scenario.messages);
    final worker = await queue.worker<Map<String, Object?>>(
      topic,
      durableName: 'stress_workers',
      concurrency: scenario.consumers,
      handler: (_, payload) async {
        await _sleep(scenario.handlerSleep);
        received.add(payload['index'] as int);
      },
    );

    final elapsed = await _time(() async {
      await received.done.timeout(
        _workerTimeout(
          scenario.messages,
          scenario.handlerSleep,
          scenario.consumers,
        ),
      );
    });

    await worker.close();
    return StressResult.completed(
      scenario: scenario,
      broker: options.broker,
      machine: options.machine,
      received: received.count,
      elapsed: elapsed,
      ackMode: 'manual',
      durability: storage == NatsJetStreamStorage.file ? 'file' : 'memory',
      publisherConfirms: 'JetStream PubAck',
      notes: modeNote,
    );
  } finally {
    await queue.close(timeout: const Duration(seconds: 5));
  }
}

Future<StressResult> _stressRabbitMqJobs(
  StressOptions options,
  StressScenario scenario, {
  required bool durable,
  required String modeNote,
}) async {
  final id = _runId();
  final topic = 'podbus.stress.rabbitmq.job.$id';
  final bus = RabbitMqMessageBus(
    config: RabbitMqMessagingConfig(
      uri: Uri.parse(
        _env('PODBUS_RABBITMQ_URL', 'amqp://guest:guest@localhost:5672'),
      ),
      exchange: 'podbus.stress.jobs.$id',
      deadLetterExchange: 'podbus.stress.jobs.dead.$id',
      durable: durable,
      prefetchCount: math.max(1, scenario.consumers),
    ),
  );

  await bus.connect();
  try {
    await _enqueueJobs(
      scenario.messages,
      scenario.producers,
      (index) => bus.enqueue(topic, _payload(index, scenario.payloadSize)),
    );

    final received = _Counter(scenario.messages);
    final worker = await bus.worker<Map<String, Object?>>(
      topic,
      concurrency: scenario.consumers,
      handler: (_, payload) async {
        await _sleep(scenario.handlerSleep);
        received.add(payload['index'] as int);
      },
    );

    final elapsed = await _time(() async {
      await received.done.timeout(
        _workerTimeout(
          scenario.messages,
          scenario.handlerSleep,
          scenario.consumers,
        ),
      );
    });

    await worker.close();
    return StressResult.completed(
      scenario: scenario,
      broker: options.broker,
      machine: options.machine,
      received: received.count,
      elapsed: elapsed,
      ackMode: 'manual',
      durability: durable ? 'persistent queue/message' : 'no',
      publisherConfirms: 'no',
      notes: modeNote,
    );
  } finally {
    await bus.close(timeout: const Duration(seconds: 5));
  }
}

Future<StressResult> _stressKafkaJobs(
  StressOptions options,
  StressScenario scenario, {
  required String modeNote,
}) async {
  final id = _runId();
  final topic = 'podbus.stress.kafka.job.$id';
  final bus = KafkaEventBus(
    config: KafkaMessagingConfig(
      brokers: [_env('PODBUS_KAFKA_BROKER', 'localhost:9092')],
      clientId: 'podbus-stress-$id',
      groupId: 'podbus-stress-$id',
      requestTimeout: const Duration(seconds: 1),
    ),
  );

  await bus.connect();
  try {
    await _enqueueJobs(
      scenario.messages,
      scenario.producers,
      (index) => bus.enqueue(topic, _payload(index, scenario.payloadSize)),
    );
    await bus.healthCheck();

    final received = _Counter(scenario.messages);
    final worker = await bus.worker<Map<String, Object?>>(
      topic,
      durableName: 'stress_workers_$id',
      concurrency: 1,
      handler: (_, payload) async {
        await _sleep(scenario.handlerSleep);
        received.add(payload['index'] as int);
      },
    );

    final elapsed = await _time(() async {
      await received.done.timeout(
        _workerTimeout(scenario.messages, scenario.handlerSleep, 1),
      );
    });

    await worker.close();
    return StressResult.completed(
      scenario: scenario,
      broker: options.broker,
      machine: options.machine,
      received: received.count,
      elapsed: elapsed,
      ackMode: 'commit',
      durability: 'event log, untuned',
      publisherConfirms: 'not configurable',
      notes: modeNote,
    );
  } finally {
    await bus.close(timeout: const Duration(seconds: 5));
  }
}

Future<StressResult> _stressNatsJetStreamDeadLetter(
  StressOptions options,
  StressScenario scenario,
) async {
  final id = _runId();
  final topic = 'podbus.failure.jetstream.$id';
  final deadLetterTopic = '$topic.dead-letter';
  final queue = _natsJetStreamQueue(id, [
    topic,
    deadLetterTopic,
  ], NatsJetStreamStorage.file);

  await queue.connect();
  try {
    final deadLetters = _Counter(scenario.messages);
    final deadWorker = await queue.worker<Map<String, Object?>>(
      deadLetterTopic,
      durableName: 'dead_workers',
      handler: (_, payload) async {
        deadLetters.add(payload['index'] as int);
      },
    );
    final worker = await queue.worker<Map<String, Object?>>(
      topic,
      durableName: 'failure_workers',
      retryPolicy: RetryPolicy(
        maxAttempts: 1,
        initialDelay: Duration.zero,
        maxDelay: Duration.zero,
      ),
      deadLetterPolicy: DeadLetterPolicy(
        enabled: true,
        destination: deadLetterTopic,
      ),
      handler: (_, _) async {
        throw StateError('failure mode');
      },
    );

    final elapsed = await _time(() async {
      await _enqueueJobs(
        scenario.messages,
        scenario.producers,
        (index) => queue.enqueue(topic, _payload(index, scenario.payloadSize)),
      );
      await deadLetters.done.timeout(_failureTimeout);
    });

    await worker.close();
    await deadWorker.close();
    return StressResult.completed(
      scenario: scenario,
      broker: options.broker,
      machine: options.machine,
      received: 0,
      deadLettered: deadLetters.count,
      elapsed: elapsed,
      ackMode: 'manual',
      durability: 'file',
      publisherConfirms: 'JetStream PubAck',
      notes: 'worker throws; DLQ observed',
    );
  } finally {
    await queue.close(timeout: const Duration(seconds: 5));
  }
}

Future<StressResult> _stressRabbitMqDeadLetter(
  StressOptions options,
  StressScenario scenario,
) async {
  final id = _runId();
  final topic = 'podbus.failure.rabbitmq.$id';
  final deadLetterTopic = '$topic.dead-letter';
  final exchange = 'podbus.failure.rabbitmq.$id';
  final deadExchange = 'podbus.failure.rabbitmq.dead.$id';
  final bus = RabbitMqMessageBus(
    config: RabbitMqMessagingConfig(
      uri: Uri.parse(
        _env('PODBUS_RABBITMQ_URL', 'amqp://guest:guest@localhost:5672'),
      ),
      exchange: exchange,
      deadLetterExchange: deadExchange,
      durable: true,
      prefetchCount: math.max(1, scenario.consumers),
    ),
  );
  final deadBus = RabbitMqMessageBus(
    config: RabbitMqMessagingConfig(
      uri: Uri.parse(
        _env('PODBUS_RABBITMQ_URL', 'amqp://guest:guest@localhost:5672'),
      ),
      exchange: deadExchange,
      deadLetterExchange: '$deadExchange.unhandled',
      durable: true,
      prefetchCount: math.max(1, scenario.consumers),
    ),
  );

  await bus.connect();
  await deadBus.connect();
  try {
    final deadLetters = _Counter(scenario.messages);
    final deadWorker = await deadBus.worker<Map<String, Object?>>(
      deadLetterTopic,
      handler: (_, payload) async {
        deadLetters.add(payload['index'] as int);
      },
    );
    final worker = await bus.worker<Map<String, Object?>>(
      topic,
      retryPolicy: RetryPolicy(
        maxAttempts: 1,
        initialDelay: Duration.zero,
        maxDelay: Duration.zero,
      ),
      deadLetterPolicy: DeadLetterPolicy(
        enabled: true,
        destination: deadLetterTopic,
      ),
      handler: (_, _) async {
        throw StateError('failure mode');
      },
    );

    final elapsed = await _time(() async {
      await _enqueueJobs(
        scenario.messages,
        scenario.producers,
        (index) => bus.enqueue(topic, _payload(index, scenario.payloadSize)),
      );
      await deadLetters.done.timeout(_failureTimeout);
    });

    await worker.close();
    await deadWorker.close();
    return StressResult.completed(
      scenario: scenario,
      broker: options.broker,
      machine: options.machine,
      received: 0,
      deadLettered: deadLetters.count,
      elapsed: elapsed,
      ackMode: 'manual',
      durability: 'persistent queue/message',
      publisherConfirms: 'no',
      notes: 'manual DLQ observed; broker-side DLX not proven',
    );
  } finally {
    await deadBus.close(timeout: const Duration(seconds: 5));
    await bus.close(timeout: const Duration(seconds: 5));
  }
}

Future<StressResult> _stressKafkaDeadLetter(
  StressOptions options,
  StressScenario scenario,
) async {
  final id = _runId();
  final topic = 'podbus.failure.kafka.$id';
  final deadLetterTopic = '$topic.dead-letter';
  final bus = KafkaEventBus(
    config: KafkaMessagingConfig(
      brokers: [_env('PODBUS_KAFKA_BROKER', 'localhost:9092')],
      clientId: 'podbus-stress-$id',
      groupId: 'podbus-stress-$id',
      requestTimeout: const Duration(seconds: 1),
    ),
  );

  await bus.connect();
  try {
    final deadLetters = _Counter(scenario.messages);
    await bus.subscribe<Map<String, Object?>>(
      deadLetterTopic,
      queueGroup: 'dead_workers_$id',
      handler: (_, payload) async {
        deadLetters.add(payload['index'] as int);
      },
    );
    final worker = await bus.worker<Map<String, Object?>>(
      topic,
      durableName: 'failure_workers_$id',
      deadLetterPolicy: DeadLetterPolicy(
        enabled: true,
        destination: deadLetterTopic,
      ),
      handler: (_, _) async {
        throw StateError('failure mode');
      },
    );

    final elapsed = await _time(() async {
      await _enqueueJobs(
        scenario.messages,
        scenario.producers,
        (index) => bus.enqueue(topic, _payload(index, scenario.payloadSize)),
      );
      await bus.healthCheck();
      await _waitForCounter(
        deadLetters,
        timeout: _failureTimeout,
        tick: () => bus.healthCheck(),
      );
    });

    await worker.close();
    return StressResult.completed(
      scenario: scenario,
      broker: options.broker,
      machine: options.machine,
      received: 0,
      deadLettered: deadLetters.count,
      elapsed: elapsed,
      ackMode: 'commit',
      durability: 'event log, untuned',
      publisherConfirms: 'not configurable',
      notes: 'worker throws; DLQ topic observed',
    );
  } finally {
    await bus.close(timeout: const Duration(seconds: 5));
  }
}

NatsJetStreamJobQueue _natsJetStreamQueue(
  String id,
  List<String> subjects,
  NatsJetStreamStorage storage,
) {
  return NatsJetStreamJobQueue(
    config: NatsMessagingConfig(
      servers: [Uri.parse(_env('PODBUS_NATS_URL', 'nats://localhost:4222'))],
      connectTimeout: const Duration(seconds: 2),
      requestTimeout: const Duration(seconds: 10),
      jetStream: NatsJetStreamConfig(
        enabled: true,
        streamName: 'PODBUS_STRESS_$id',
        subjects: subjects,
        storage: storage,
      ),
    ),
    fetchTimeout: Duration(
      milliseconds: _envInt(
        Platform.environment,
        'PODBUS_JETSTREAM_FETCH_TIMEOUT_MS',
        200,
      ),
    ),
    fetchBatchSize: _envInt(
      Platform.environment,
      'PODBUS_JETSTREAM_FETCH_BATCH_SIZE',
      1,
    ),
  );
}

Future<StressResult> _unsupported(
  StressOptions options,
  StressScenario scenario,
  String reason,
) async {
  return StressResult.skipped(
    scenario: scenario,
    broker: options.broker,
    machine: options.machine,
    reason: reason,
  );
}

Future<void> _enqueueJobs(
  int count,
  int producers,
  Future<void> Function(int index) enqueue,
) {
  return _publishWindowed(count, producers, enqueue);
}

Future<void> _publishWindowed(
  int count,
  int concurrency,
  Future<void> Function(int index) publish,
) async {
  for (var start = 0; start < count; start += concurrency) {
    final end = (start + concurrency) > count ? count : start + concurrency;
    await Future.wait([
      for (var index = start; index < end; index += 1) publish(index),
    ]);
  }
}

Map<String, Object?> _payload(int index, int targetBytes) {
  final paddingLength = math.max(0, targetBytes - 64);
  return {
    'index': index,
    'createdAt': DateTime.now().toUtc().toIso8601String(),
    if (paddingLength > 0) 'padding': 'x' * paddingLength,
  };
}

Future<Duration> _time(Future<void> Function() action) async {
  final stopwatch = Stopwatch()..start();
  await action();
  stopwatch.stop();
  return stopwatch.elapsed;
}

Future<void> _sleep(Duration duration) async {
  if (duration > Duration.zero) {
    await Future<void>.delayed(duration);
  }
}

Future<void> _waitForCounter(
  _Counter counter, {
  required Duration timeout,
  Future<void> Function()? tick,
}) async {
  final deadline = DateTime.now().add(timeout);
  while (!counter.isDone) {
    if (DateTime.now().isAfter(deadline)) {
      throw TimeoutException('Counter did not reach ${counter.expected}.');
    }
    await tick?.call();
    if (!counter.isDone) {
      await Future<void>.delayed(const Duration(milliseconds: 50));
    }
  }
}

String _env(String key, String fallback) =>
    Platform.environment[key] ?? fallback;

BrokerEndpoint brokerEndpointForTransport(
  String transport,
  Map<String, String> environment,
) {
  return switch (transport) {
    'nats' || 'jetstream' => _endpointFromUri(
      environment['PODBUS_NATS_URL'] ?? 'nats://localhost:4222',
      defaultPort: 4222,
    ),
    'rabbitmq' => _endpointFromUri(
      environment['PODBUS_RABBITMQ_URL'] ?? 'amqp://guest:guest@localhost:5672',
      defaultPort: 5672,
    ),
    'kafka' => _endpointFromHostPort(
      environment['PODBUS_KAFKA_BROKER'] ?? 'localhost:9092',
      defaultPort: 9092,
    ),
    _ => throw ArgumentError('Unknown stress transport: $transport'),
  };
}

Future<String?> _brokerUnavailableReason(
  String transport,
  Map<String, String> environment,
) async {
  if (environment['PODBUS_STRESS_CHECK_BROKERS'] == 'false') {
    return null;
  }

  final endpoint = brokerEndpointForTransport(transport, environment);
  try {
    final socket = await Socket.connect(
      endpoint.host,
      endpoint.port,
      timeout: const Duration(milliseconds: 750),
    );
    socket.destroy();
    return null;
  } on Object catch (error) {
    return 'broker unavailable at ${endpoint.host}:${endpoint.port}: $error';
  }
}

BrokerEndpoint _endpointFromUri(String value, {required int defaultPort}) {
  final uri = Uri.parse(value);
  return BrokerEndpoint(uri.host, uri.hasPort ? uri.port : defaultPort);
}

BrokerEndpoint _endpointFromHostPort(String value, {required int defaultPort}) {
  final first = value.split(',').first.trim();
  final uri = Uri.tryParse(first.contains('://') ? first : 'tcp://$first');
  if (uri == null || uri.host.isEmpty) {
    throw ArgumentError('Invalid broker endpoint: $value');
  }
  return BrokerEndpoint(uri.host, uri.hasPort ? uri.port : defaultPort);
}

int _envInt(Map<String, String> environment, String key, int fallback) {
  return int.tryParse(environment[key] ?? '') ?? fallback;
}

List<int> _envIntList(
  Map<String, String> environment,
  String key,
  List<int> fallback,
) {
  return [
    for (final value in _envList(environment, key, []))
      if (int.tryParse(value) != null) int.parse(value),
  ].nonEmptyOr(fallback);
}

List<T> _envEnumList<T>(
  Map<String, String> environment,
  String key,
  T Function(String value) parse,
  List<T> fallback, {
  String? singleValueKey,
}) {
  final rawValues = _envList(environment, key, const []);
  final values = rawValues.isNotEmpty
      ? rawValues
      : singleValueKey == null
      ? const <String>[]
      : _envList(environment, singleValueKey, const []);
  return [for (final value in values) parse(value)].nonEmptyOr(fallback);
}

List<String> _envList(
  Map<String, String> environment,
  String key,
  List<String> fallback,
) {
  final value = environment[key];
  if (value == null || value.trim().isEmpty) {
    return fallback;
  }
  return [
    for (final item in value.split(','))
      if (item.trim().isNotEmpty) item.trim(),
  ];
}

String _runId() => DateTime.now().microsecondsSinceEpoch.toString();

String _machineSummary() {
  return [
    Platform.operatingSystem,
    'cpus=${Platform.numberOfProcessors}',
    'ram=${_memorySummary()}',
  ].join(' ');
}

String _memorySummary() {
  if (Platform.isMacOS) {
    final result = Process.runSync('sysctl', ['-n', 'hw.memsize']);
    final bytes = int.tryParse((result.stdout as String).trim());
    if (bytes != null) {
      return '${(bytes / (1024 * 1024 * 1024)).toStringAsFixed(1)}GB';
    }
  }
  if (Platform.isLinux) {
    final file = File('/proc/meminfo');
    if (file.existsSync()) {
      final match = RegExp(
        r'^MemTotal:\s+(\d+)\s+kB',
        multiLine: true,
      ).firstMatch(file.readAsStringSync());
      final kb = int.tryParse(match?.group(1) ?? '');
      if (kb != null) {
        return '${(kb / (1024 * 1024)).toStringAsFixed(1)}GB';
      }
    }
  }
  return 'unknown';
}

Duration _workerTimeout(int messages, Duration handlerSleep, int concurrency) {
  final effectiveConcurrency = math.max(1, concurrency);
  final batches = (messages / effectiveConcurrency).ceil();
  final handlerMicros = handlerSleep.inMicroseconds * batches * 3;
  return Duration(microseconds: handlerMicros) + const Duration(seconds: 90);
}

const _eventTimeout = Duration(seconds: 90);
const _failureTimeout = Duration(seconds: 90);

final class _Counter {
  _Counter(this.expected);

  final int expected;
  final _seen = <int>{};
  final _done = Completer<void>();

  int get count => _seen.length;

  Future<void> get done => _done.future;

  bool get isDone => _done.isCompleted;

  void add(int index) {
    _seen.add(index);
    if (_seen.length >= expected && !_done.isCompleted) {
      _done.complete();
    }
  }
}

final class StressResult {
  const StressResult({
    required this.scenario,
    required this.broker,
    required this.machine,
    required this.status,
    required this.received,
    required this.deadLettered,
    required this.elapsed,
    required this.ackMode,
    required this.durability,
    required this.publisherConfirms,
    required this.notes,
  });

  factory StressResult.completed({
    required StressScenario scenario,
    required String broker,
    required String machine,
    required int received,
    required Duration elapsed,
    required String ackMode,
    required String durability,
    required String publisherConfirms,
    required String notes,
    int deadLettered = 0,
  }) {
    return StressResult(
      scenario: scenario,
      broker: broker,
      machine: machine,
      status: 'ok',
      received: received,
      deadLettered: deadLettered,
      elapsed: elapsed,
      ackMode: ackMode,
      durability: durability,
      publisherConfirms: publisherConfirms,
      notes: notes,
    );
  }

  factory StressResult.skipped({
    required StressScenario scenario,
    required String broker,
    required String machine,
    required String reason,
  }) {
    return StressResult(
      scenario: scenario,
      broker: broker,
      machine: machine,
      status: 'skipped',
      received: 0,
      deadLettered: 0,
      elapsed: Duration.zero,
      ackMode: '-',
      durability: '-',
      publisherConfirms: '-',
      notes: reason,
    );
  }

  factory StressResult.failed({
    required StressScenario scenario,
    required String broker,
    required String machine,
    required Object error,
    required StackTrace stackTrace,
  }) {
    return StressResult(
      scenario: scenario,
      broker: broker,
      machine: machine,
      status: 'failed',
      received: 0,
      deadLettered: 0,
      elapsed: Duration.zero,
      ackMode: '-',
      durability: '-',
      publisherConfirms: '-',
      notes: '$error',
    );
  }

  static const markdownHeader =
      '| Mode | Transport | Payload | Producers | Consumers | Ack | Durability | Confirms | Status | Received | DLQ | Elapsed | Throughput | Notes |';
  static const _markdownSeparator =
      '| --- | --- | ---: | ---: | ---: | --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- |';

  final StressScenario scenario;
  final String broker;
  final String machine;
  final String status;
  final int received;
  final int deadLettered;
  final Duration elapsed;
  final String ackMode;
  final String durability;
  final String publisherConfirms;
  final String notes;

  double get throughput {
    if (elapsed == Duration.zero) {
      return 0;
    }
    return received / (elapsed.inMicroseconds / Duration.microsecondsPerSecond);
  }

  String formatMarkdownRow() {
    return [
      '',
      scenario.mode.name,
      scenario.transport,
      scenario.payloadSize.toString(),
      scenario.producers.toString(),
      scenario.consumers.toString(),
      ackMode,
      durability,
      publisherConfirms,
      status,
      received.toString(),
      deadLettered.toString(),
      '${elapsed.inMilliseconds}ms',
      throughput.toStringAsFixed(1),
      _escapeMarkdown(notes),
      '',
    ].join(' | ');
  }

  String formatCompact() {
    return '${scenario.mode.name}/${scenario.transport}: $status '
        'received=$received/${scenario.messages} '
        'dlq=$deadLettered elapsed=${elapsed.inMilliseconds}ms '
        'throughput=${throughput.toStringAsFixed(1)} msg/s';
  }

  static String get markdownSeparator => _markdownSeparator;
}

extension<T> on List<T> {
  List<T> nonEmptyOr(List<T> fallback) => isEmpty ? fallback : this;
}

String _escapeMarkdown(String value) => value.replaceAll('|', r'\|');
