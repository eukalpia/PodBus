import 'dart:async';
import 'dart:io';

import 'package:podbus_kafka/podbus_kafka.dart';
import 'package:podbus_nats/podbus_nats.dart';
import 'package:podbus_rabbitmq/podbus_rabbitmq.dart';

Future<void> main() async {
  final count = _envInt('PODBUS_STRESS_MESSAGES', 2000);
  final concurrency = _envInt('PODBUS_STRESS_CONCURRENCY', 100);
  final transports = _envList('PODBUS_STRESS_TRANSPORTS', [
    'nats',
    'jetstream',
    'rabbitmq',
    'kafka',
  ]);

  final results = <StressResult>[];
  for (final transport in transports) {
    final result = switch (transport) {
      'nats' => await _stressNatsCore(count, concurrency),
      'jetstream' => await _stressNatsJetStream(count, concurrency),
      'rabbitmq' => await _stressRabbitMq(count, concurrency),
      'kafka' => await _stressKafka(count, concurrency),
      _ => throw ArgumentError('Unknown stress transport: $transport'),
    };
    results.add(result);
    stdout.writeln(result.format());
  }

  stdout.writeln('\nSummary');
  for (final result in results) {
    stdout.writeln(result.formatCompact());
  }
}

Future<StressResult> _stressNatsCore(int count, int concurrency) async {
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
    final received = _Counter(count);
    await bus.subscribe<Map<String, Object?>>(
      subject,
      handler: (_, payload) async {
        received.add(payload['index'] as int);
      },
    );
    await bus.healthCheck();

    final publishTime = Stopwatch()..start();
    await _publishWindowed(
      count,
      concurrency,
      (index) => bus.publish(subject, {'index': index}),
    );
    publishTime.stop();

    final totalTime = Stopwatch()..start();
    await received.done.timeout(const Duration(seconds: 30));
    totalTime.stop();

    return StressResult(
      transport: 'nats',
      messages: count,
      published: count,
      received: received.count,
      publishElapsed: publishTime.elapsed,
      totalElapsed: publishTime.elapsed + totalTime.elapsed,
    );
  } finally {
    await bus.close(timeout: const Duration(seconds: 5));
  }
}

Future<StressResult> _stressNatsJetStream(int count, int concurrency) async {
  final id = _runId();
  final topic = 'podbus.stress.jetstream.$id';
  final queue = NatsJetStreamJobQueue(
    config: NatsMessagingConfig(
      servers: [Uri.parse(_env('PODBUS_NATS_URL', 'nats://localhost:4222'))],
      connectTimeout: const Duration(seconds: 2),
      requestTimeout: const Duration(seconds: 10),
      jetStream: NatsJetStreamConfig(
        enabled: true,
        streamName: 'PODBUS_STRESS_$id',
        subjects: [topic],
        storage: NatsJetStreamStorage.memory,
      ),
    ),
    fetchTimeout: const Duration(milliseconds: 200),
  );

  await queue.connect();
  try {
    final received = _Counter(count);
    final worker = await queue.worker<Map<String, Object?>>(
      topic,
      durableName: 'stress_workers',
      concurrency: concurrency.clamp(1, 32),
      handler: (_, payload) async {
        received.add(payload['index'] as int);
      },
    );

    final publishTime = Stopwatch()..start();
    await _publishWindowed(
      count,
      concurrency,
      (index) => queue.enqueue(topic, {'index': index}),
    );
    publishTime.stop();

    final totalTime = Stopwatch()..start();
    await received.done.timeout(const Duration(seconds: 60));
    totalTime.stop();

    await worker.close();
    return StressResult(
      transport: 'jetstream',
      messages: count,
      published: count,
      received: received.count,
      publishElapsed: publishTime.elapsed,
      totalElapsed: publishTime.elapsed + totalTime.elapsed,
    );
  } finally {
    await queue.close(timeout: const Duration(seconds: 5));
  }
}

Future<StressResult> _stressRabbitMq(int count, int concurrency) async {
  final id = _runId();
  final subject = 'podbus.stress.rabbitmq.$id';
  final bus = RabbitMqMessageBus(
    config: RabbitMqMessagingConfig(
      uri: Uri.parse(
        _env('PODBUS_RABBITMQ_URL', 'amqp://guest:guest@localhost:5672'),
      ),
      exchange: 'podbus.stress.events.$id',
      deadLetterExchange: 'podbus.stress.dead.$id',
      durable: false,
      prefetchCount: concurrency.clamp(1, 200),
    ),
  );

  await bus.connect();
  try {
    final received = _Counter(count);
    await bus.subscribe<Map<String, Object?>>(
      subject,
      queueGroup: 'stress',
      handler: (_, payload) async {
        received.add(payload['index'] as int);
      },
    );

    final publishTime = Stopwatch()..start();
    await _publishWindowed(
      count,
      concurrency,
      (index) => bus.publish(subject, {'index': index}),
    );
    publishTime.stop();

    final totalTime = Stopwatch()..start();
    await received.done.timeout(const Duration(seconds: 60));
    totalTime.stop();

    return StressResult(
      transport: 'rabbitmq',
      messages: count,
      published: count,
      received: received.count,
      publishElapsed: publishTime.elapsed,
      totalElapsed: publishTime.elapsed + totalTime.elapsed,
    );
  } finally {
    await bus.close(timeout: const Duration(seconds: 5));
  }
}

Future<StressResult> _stressKafka(int count, int concurrency) async {
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
    final received = _Counter(count);
    await bus.subscribe<Map<String, Object?>>(
      topic,
      handler: (_, payload) async {
        received.add(payload['index'] as int);
      },
    );

    final publishTime = Stopwatch()..start();
    await _publishWindowed(
      count,
      concurrency,
      (index) => bus.publish(topic, {'index': index}),
    );
    await bus.healthCheck();
    publishTime.stop();

    final totalTime = Stopwatch()..start();
    await received.done.timeout(const Duration(seconds: 90));
    totalTime.stop();

    return StressResult(
      transport: 'kafka',
      messages: count,
      published: count,
      received: received.count,
      publishElapsed: publishTime.elapsed,
      totalElapsed: publishTime.elapsed + totalTime.elapsed,
    );
  } finally {
    await bus.close(timeout: const Duration(seconds: 5));
  }
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

String _env(String key, String fallback) =>
    Platform.environment[key] ?? fallback;

int _envInt(String key, int fallback) {
  return int.tryParse(Platform.environment[key] ?? '') ?? fallback;
}

List<String> _envList(String key, List<String> fallback) {
  final value = Platform.environment[key];
  if (value == null || value.trim().isEmpty) {
    return fallback;
  }
  return [
    for (final item in value.split(','))
      if (item.trim().isNotEmpty) item.trim(),
  ];
}

String _runId() => DateTime.now().microsecondsSinceEpoch.toString();

final class _Counter {
  _Counter(this.expected);

  final int expected;
  final _seen = <int>{};
  final _done = Completer<void>();

  int get count => _seen.length;

  Future<void> get done => _done.future;

  void add(int index) {
    _seen.add(index);
    if (_seen.length >= expected && !_done.isCompleted) {
      _done.complete();
    }
  }
}

final class StressResult {
  const StressResult({
    required this.transport,
    required this.messages,
    required this.published,
    required this.received,
    required this.publishElapsed,
    required this.totalElapsed,
  });

  final String transport;
  final int messages;
  final int published;
  final int received;
  final Duration publishElapsed;
  final Duration totalElapsed;

  double get throughput {
    final seconds =
        totalElapsed.inMicroseconds / Duration.microsecondsPerSecond;
    return seconds == 0 ? 0 : received / seconds;
  }

  String format() {
    return [
      '\n$transport',
      'messages=$messages',
      'published=$published',
      'received=$received',
      'publishElapsed=${publishElapsed.inMilliseconds}ms',
      'totalElapsed=${totalElapsed.inMilliseconds}ms',
      'throughput=${throughput.toStringAsFixed(1)} msg/s',
    ].join('\n');
  }

  String formatCompact() {
    return '$transport: $received/$messages in '
        '${totalElapsed.inMilliseconds}ms '
        '(${throughput.toStringAsFixed(1)} msg/s)';
  }
}
