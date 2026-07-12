from pathlib import Path
import re

stress_path = Path('tool/stress_transports.dart')
source = stress_path.read_text()


def replace_function(start_name: str, next_name: str, replacement: str) -> None:
    global source
    pattern = (
        rf'Future<StressResult> {re.escape(start_name)}\('
        rf'[\s\S]*?(?=Future<StressResult> {re.escape(next_name)}\()'
    )
    source, count = re.subn(pattern, replacement, source, count=1)
    if count != 1:
        raise SystemExit(f'failed to replace {start_name}')


replace_function(
    '_stressNatsCoreEvents',
    '_stressRabbitMqEvents',
    r'''Future<StressResult> _stressNatsCoreEvents(
  StressOptions options,
  StressScenario scenario,
) async {
  final id = _runId();
  final subject = 'podbus.stress.nats.$id';
  final config = NatsMessagingConfig(
    servers: [Uri.parse(_env('PODBUS_NATS_URL', 'nats://localhost:4222'))],
    connectTimeout: const Duration(seconds: 2),
    requestTimeout: const Duration(seconds: 10),
  );
  final subscriber = NatsMessageBus(config: config);
  final publisher = NatsMessageBus(config: config);

  await Future.wait([subscriber.connect(), publisher.connect()]);
  try {
    final received = _Counter(scenario.messages);
    for (var i = 0; i < scenario.consumers; i += 1) {
      await subscriber.subscribe<Map<String, Object?>>(
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
        (index) => publisher.publish(
          subject,
          _payload(index, scenario.payloadSize),
        ),
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
      notes: 'at-most-once Core pub/sub; isolated publish and consume sockets',
    );
  } finally {
    await Future.wait([
      publisher.close(timeout: const Duration(seconds: 5)),
      subscriber.close(timeout: const Duration(seconds: 5)),
    ]);
  }
}

''',
)

replace_function(
    '_stressRabbitMqEvents',
    '_stressKafkaEvents',
    r'''Future<StressResult> _stressRabbitMqEvents(
  StressOptions options,
  StressScenario scenario, {
  required bool durable,
}) async {
  final id = _runId();
  final subject = 'podbus.stress.rabbitmq.$id';

  RabbitMqMessagingConfig config(String connectionName) {
    return RabbitMqMessagingConfig(
      uri: Uri.parse(
        _env('PODBUS_RABBITMQ_URL', 'amqp://guest:guest@localhost:5672'),
      ),
      exchange: 'podbus.stress.events.$id',
      deadLetterExchange: 'podbus.stress.dead.$id',
      durable: durable,
      prefetchCount: math.max(1, scenario.consumers),
      connectionName: connectionName,
    );
  }

  final subscriber = RabbitMqMessageBus(
    config: config('podbus-stress-consumer-$id'),
  );
  final publisher = RabbitMqMessageBus(
    config: config('podbus-stress-publisher-$id'),
  );

  await Future.wait([subscriber.connect(), publisher.connect()]);
  try {
    final received = _Counter(scenario.messages);
    for (var i = 0; i < scenario.consumers; i += 1) {
      await subscriber.subscribe<Map<String, Object?>>(
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
        (index) => publisher.publish(
          subject,
          _payload(index, scenario.payloadSize),
        ),
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
      publisherConfirms: 'AMQP confirm',
      notes: durable
          ? 'publisher confirms; isolated publish and consume connections'
          : 'non-persistent fast path; isolated connections',
    );
  } finally {
    await Future.wait([
      publisher.close(timeout: const Duration(seconds: 5)),
      subscriber.close(timeout: const Duration(seconds: 5)),
    ]);
  }
}

''',
)

replace_function(
    '_stressNatsJetStreamJobs',
    '_stressRabbitMqJobs',
    r'''Future<StressResult> _stressNatsJetStreamJobs(
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

    late final Duration elapsed;
    try {
      elapsed = await _time(() async {
        await _enqueueJobs(
          scenario.messages,
          scenario.producers,
          (index) => queue.enqueue(
            topic,
            _payload(index, scenario.payloadSize),
          ),
        );
        await received.done.timeout(
          _workerTimeout(
            scenario.messages,
            scenario.handlerSleep,
            scenario.consumers,
          ),
        );
      });
    } finally {
      await worker.close();
    }

    return StressResult.completed(
      scenario: scenario,
      broker: options.broker,
      machine: options.machine,
      received: received.count,
      elapsed: elapsed,
      ackMode: 'manual',
      durability: storage == NatsJetStreamStorage.file ? 'file' : 'memory',
      publisherConfirms: 'JetStream PubAck',
      notes: '$modeNote; end-to-end enqueue and delivery',
    );
  } finally {
    await queue.close(timeout: const Duration(seconds: 5));
  }
}

''',
)

replace_function(
    '_stressRabbitMqJobs',
    '_stressKafkaJobs',
    r'''Future<StressResult> _stressRabbitMqJobs(
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
      connectionName: 'podbus-stress-jobs-$id',
    ),
  );

  await bus.connect();
  try {
    final received = _Counter(scenario.messages);
    final worker = await bus.worker<Map<String, Object?>>(
      topic,
      concurrency: scenario.consumers,
      handler: (_, payload) async {
        await _sleep(scenario.handlerSleep);
        received.add(payload['index'] as int);
      },
    );

    late final Duration elapsed;
    try {
      elapsed = await _time(() async {
        await _enqueueJobs(
          scenario.messages,
          scenario.producers,
          (index) => bus.enqueue(
            topic,
            _payload(index, scenario.payloadSize),
          ),
        );
        await received.done.timeout(
          _workerTimeout(
            scenario.messages,
            scenario.handlerSleep,
            scenario.consumers,
          ),
        );
      });
    } finally {
      await worker.close();
    }

    return StressResult.completed(
      scenario: scenario,
      broker: options.broker,
      machine: options.machine,
      received: received.count,
      elapsed: elapsed,
      ackMode: 'manual',
      durability: durable ? 'persistent queue/message' : 'no',
      publisherConfirms: 'AMQP confirm',
      notes: '$modeNote; end-to-end enqueue and delivery',
    );
  } finally {
    await bus.close(timeout: const Duration(seconds: 5));
  }
}

''',
)

stress_path.write_text(source)

workflow_path = Path('.github/workflows/stress.yml')
workflow = workflow_path.read_text()
old = '''          dart run tool/stress_transports.dart | tee "$result"
          cat "$result" >> "$GITHUB_STEP_SUMMARY"
          if grep -Eq '\\| (failed|skipped) \\|' "$result"; then
            echo "The requested stress scenario did not complete successfully." >&2
            exit 1
          fi'''
new = '''          set +e
          dart run tool/stress_transports.dart 2>&1 | tee "$result"
          status=${PIPESTATUS[0]}
          set -e
          cat "$result" >> "$GITHUB_STEP_SUMMARY"
          if [[ $status -ne 0 ]]; then
            echo "The stress harness exited with status $status." >&2
            exit "$status"
          fi
          if grep -Eq '\\| (failed|skipped) \\|' "$result"; then
            echo "The requested stress scenario did not complete successfully." >&2
            exit 1
          fi'''
if old not in workflow:
    raise SystemExit('stress workflow command block not found')
workflow_path.write_text(workflow.replace(old, new, 1))
