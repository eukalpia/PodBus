import 'dart:async';

import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_nats/podbus_nats.dart';
import 'package:podbus_nats/src/nats_jetstream_adapter.dart';
import 'package:test/test.dart';

void main() {
  group('NATS JetStream resilience', () {
    test('sends in-progress heartbeats while a long handler runs', () async {
      final adapter = _Adapter();
      final queue = NatsJetStreamJobQueue(
        config: _config(),
        jetStreamAdapter: adapter,
        fetchTimeout: const Duration(milliseconds: 10),
        inProgressInterval: const Duration(milliseconds: 20),
      );
      await queue.connect();

      final release = Completer<void>();
      final started = Completer<void>();
      final worker = await queue.worker<Map<String, Object?>>(
        'jobs.long',
        durableName: 'long-v1',
        handler: (_, _) async {
          started.complete();
          await release.future;
        },
      );
      final message = _Message('jobs.long', {'id': 1});
      adapter.consumer.add(message);

      await started.future.timeout(_timeout);
      await _waitFor(() => message.inProgressCalls >= 2);
      expect(message.ackCalls, 0);
      release.complete();
      await _waitFor(() => message.ackCalls == 1);

      await worker.close();
      await queue.close();
    });

    test('recovers fetch loop after repeated transient failures', () async {
      final adapter = _Adapter()..consumer.fetchFailuresRemaining = 4;
      final queue = NatsJetStreamJobQueue(
        config: _config(),
        jetStreamAdapter: adapter,
        fetchTimeout: const Duration(milliseconds: 5),
        workerRecoveryInitialDelay: const Duration(milliseconds: 1),
        workerRecoveryMaxDelay: const Duration(milliseconds: 4),
      );
      await queue.connect();

      final handled = Completer<void>();
      final worker = await queue.worker<Map<String, Object?>>(
        'jobs.recover',
        durableName: 'recover-v1',
        handler: (_, _) async => handled.complete(),
      );
      final message = _Message('jobs.recover', {'id': 1});
      adapter.consumer.add(message);

      await handled.future.timeout(_timeout);
      expect(adapter.consumer.fetchFailures, 4);
      expect(message.ackCalls, 1);
      final health = await queue.healthCheck();
      expect(health.status, HealthStatus.healthy);

      await worker.close();
      await queue.close();
    });

    test('never exceeds configured handler concurrency under burst load', () async {
      final adapter = _Adapter();
      final queue = NatsJetStreamJobQueue(
        config: _config(),
        jetStreamAdapter: adapter,
        fetchTimeout: const Duration(milliseconds: 5),
        fetchBatchSize: 8,
        inProgressInterval: const Duration(milliseconds: 10),
      );
      await queue.connect();

      const concurrency = 4;
      var active = 0;
      var maxActive = 0;
      var completed = 0;
      final done = Completer<void>();
      final worker = await queue.worker<Map<String, Object?>>(
        'jobs.burst',
        durableName: 'burst-v1',
        concurrency: concurrency,
        handler: (_, _) async {
          active += 1;
          if (active > maxActive) maxActive = active;
          await Future<void>.delayed(const Duration(milliseconds: 15));
          active -= 1;
          completed += 1;
          if (completed == 100 && !done.isCompleted) done.complete();
        },
      );
      final messages = [
        for (var index = 0; index < 100; index += 1)
          _Message('jobs.burst', {'id': index}),
      ];
      for (final message in messages) {
        adapter.consumer.add(message);
      }

      await done.future.timeout(const Duration(seconds: 5));
      expect(maxActive, lessThanOrEqualTo(concurrency));
      expect(messages.every((message) => message.ackCalls == 1), isTrue);

      await worker.close();
      await queue.close();
    });

    test('graceful close waits for an active handler before draining', () async {
      final adapter = _Adapter();
      final queue = NatsJetStreamJobQueue(
        config: _config(),
        jetStreamAdapter: adapter,
        fetchTimeout: const Duration(milliseconds: 5),
        inProgressInterval: const Duration(milliseconds: 10),
        messagingConfig: MessagingConfig(
          shutdownTimeout: const Duration(seconds: 2),
        ),
      );
      await queue.connect();

      final started = Completer<void>();
      final release = Completer<void>();
      await queue.worker<Map<String, Object?>>(
        'jobs.shutdown',
        durableName: 'shutdown-v1',
        handler: (_, _) async {
          started.complete();
          await release.future;
        },
      );
      final message = _Message('jobs.shutdown', {'id': 1});
      adapter.consumer.add(message);
      await started.future.timeout(_timeout);

      var closed = false;
      final close = queue.close().then((_) => closed = true);
      await Future<void>.delayed(const Duration(milliseconds: 40));
      expect(closed, isFalse);
      expect(message.inProgressCalls, greaterThan(0));
      release.complete();
      await close.timeout(_timeout);
      expect(message.ackCalls, 1);
      expect(adapter.drained, isTrue);
    });

    test('duplicate redelivery exposes delivery count to idempotent handlers', () async {
      final adapter = _Adapter();
      final queue = NatsJetStreamJobQueue(
        config: _config(),
        jetStreamAdapter: adapter,
        fetchTimeout: const Duration(milliseconds: 5),
      );
      await queue.connect();

      final attempts = <int>[];
      final done = Completer<void>();
      final worker = await queue.worker<Map<String, Object?>>(
        'jobs.duplicate',
        durableName: 'duplicate-v1',
        handler: (context, _) async {
          attempts.add(context.attempt);
          if (attempts.length == 2) done.complete();
        },
      );
      adapter.consumer
        ..add(_Message('jobs.duplicate', {'id': 7}, deliveryCount: 1))
        ..add(_Message('jobs.duplicate', {'id': 7}, deliveryCount: 2));

      await done.future.timeout(_timeout);
      expect(attempts, [1, 2]);

      await worker.close();
      await queue.close();
    });
  });
}

const _timeout = Duration(seconds: 3);

Future<void> _waitFor(bool Function() condition) async {
  final deadline = DateTime.now().add(_timeout);
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) {
      throw TimeoutException('Condition was not met.', _timeout);
    }
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}

NatsMessagingConfig _config() => NatsMessagingConfig(
  servers: [Uri.parse('nats://localhost:4222')],
  jetStream: const NatsJetStreamConfig(
    enabled: true,
    streamName: 'PODBUS_RESILIENCE',
    subjects: ['jobs.>'],
    storage: NatsJetStreamStorage.memory,
  ),
);

Map<String, String> get _headers => {
  PodBusWireHeaders.contentType: JsonMessageCodec.contentType,
  PodBusWireHeaders.schemaVersion: '1',
};

final class _Adapter implements NatsJetStreamAdapter {
  final consumer = _Consumer();
  bool connected = false;
  bool drained = false;

  @override
  bool get isConnected => connected;

  @override
  Future<void> connect(NatsMessagingConfig config) async => connected = true;

  @override
  Future<void> close() async => connected = false;

  @override
  Future<void> drain() async => drained = true;

  @override
  Future<void> flush() async {}

  @override
  Future<void> createOrUpdateStream(NatsJetStreamConfig config) async {}

  @override
  Future<NatsJetStreamConsumer> createOrUpdateConsumer({
    required String streamName,
    required String consumerName,
    required String topic,
  }) async => consumer;

  @override
  Future<NatsJetStreamPublishAck> publish(
    String subject,
    List<int> bytes, {
    required Duration timeout,
    String? messageId,
    Map<String, String> headers = const {},
  }) async => const NatsJetStreamPublishAck(
    stream: 'PODBUS_RESILIENCE',
    sequence: 1,
    duplicate: false,
  );
}

final class _Consumer implements NatsJetStreamConsumer {
  final _controller = StreamController<_Message>();
  int fetchFailuresRemaining = 0;
  int fetchFailures = 0;

  void add(_Message message) => _controller.add(message);

  @override
  Future<List<NatsJetStreamMessage>> fetch({
    required int batch,
    required Duration timeout,
  }) async {
    if (fetchFailuresRemaining > 0) {
      fetchFailuresRemaining -= 1;
      fetchFailures += 1;
      throw const MessagingConnectionException('temporary fetch failure');
    }
    final messages = <NatsJetStreamMessage>[];
    for (var index = 0; index < batch; index += 1) {
      try {
        messages.add(await _controller.stream.first.timeout(timeout));
      } on TimeoutException {
        break;
      }
    }
    return messages;
  }
}

final class _Message implements NatsJetStreamMessage {
  _Message(this.subject, Map<String, Object?> payload, {this.deliveryCount = 1})
    : bytes = JsonMessageCodec().encode(payload).then((value) => value.bytes) as dynamic;

  _Message.raw(this.subject, this.bytes, {this.deliveryCount = 1});

  @override
  final String subject;

  @override
  final List<int> bytes;

  @override
  Map<String, String> get headers => _headers;

  @override
  final int deliveryCount;

  @override
  int? get streamSequence => 1;

  @override
  int? get consumerSequence => 1;

  int ackCalls = 0;
  int nakCalls = 0;
  int termCalls = 0;
  int inProgressCalls = 0;

  @override
  Future<bool> ack() async {
    ackCalls += 1;
    return true;
  }

  @override
  Future<bool> nak({Duration? delay}) async {
    nakCalls += 1;
    return true;
  }

  @override
  Future<bool> term() async {
    termCalls += 1;
    return true;
  }

  @override
  Future<bool> inProgress() async {
    inProgressCalls += 1;
    return true;
  }
}
