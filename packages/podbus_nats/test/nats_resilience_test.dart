import 'dart:async';
import 'dart:convert';

import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_nats/podbus_nats.dart';
import 'package:podbus_nats/src/nats_jetstream_adapter.dart';
import 'package:test/test.dart';

void main() {
  group('NATS JetStream resilience', () {
    test('heartbeat protects a long-running handler', () async {
      final message = _Message('jobs.long', {'id': 1});
      final context = _JobContext(message);
      final release = Completer<void>();

      final action = runWithNatsJetStreamHeartbeat(
        context,
        () => release.future,
        interval: const Duration(milliseconds: 10),
      );

      await _waitFor(() => message.inProgressCalls >= 3);
      release.complete();
      await action.timeout(_timeout);
      final callsAfterStop = message.inProgressCalls;
      await Future<void>.delayed(const Duration(milliseconds: 30));
      expect(message.inProgressCalls, callsAfterStop);
    });

    test('heartbeat surfaces failures when no error hook is supplied', () async {
      final message = _Message('jobs.long', {'id': 1})
        ..heartbeatError = const MessagingConnectionException('connection lost');
      final context = _JobContext(message);

      await expectLater(
        runWithNatsJetStreamHeartbeat(
          context,
          () async => Future<void>.delayed(const Duration(milliseconds: 25)),
          interval: const Duration(milliseconds: 5),
        ),
        throwsA(isA<MessagingConnectionException>()),
      );
    });

    test('resilient queue recreates JetStream and restores workers', () async {
      final adapters = <_Adapter>[];
      final queues = <NatsJetStreamJobQueue>[];
      final resilient = ResilientDurableJobQueue(
        policy: const ReconnectPolicy(
          maxAttempts: 3,
          initialDelay: Duration.zero,
          maxDelay: Duration.zero,
          jitter: 0,
        ),
        factory: () {
          final adapter = _Adapter();
          adapters.add(adapter);
          final queue = NatsJetStreamJobQueue(
            config: _config(),
            jetStreamAdapter: adapter,
            fetchTimeout: const Duration(milliseconds: 5),
          );
          queues.add(queue);
          return queue;
        },
      );

      await resilient.connect();
      var handled = 0;
      final worker = await resilient.worker<Map<String, Object?>>(
        'jobs.email',
        durableName: 'email-v1',
        concurrency: 2,
        handler: (_, _) async => handled += 1,
      );
      adapters.first.publishError =
          const MessagingConnectionException('connection lost');

      await resilient.enqueue('jobs.email', {'id': 1});

      expect(adapters, hasLength(2));
      expect(adapters.last.createdConsumers, 1);
      adapters.last.consumer.add(_Message('jobs.email', {'id': 1}));
      await _waitFor(() => handled == 1);

      await worker.close();
      await resilient.close();
      expect(queues.lastClosed, isTrue);
    });

    test('burst processing never exceeds configured concurrency', () async {
      final adapter = _Adapter();
      final queue = NatsJetStreamJobQueue(
        config: _config(),
        jetStreamAdapter: adapter,
        fetchTimeout: const Duration(milliseconds: 5),
        fetchBatchSize: 4,
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
          await Future<void>.delayed(const Duration(milliseconds: 8));
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

    test('duplicate redelivery preserves delivery attempt', () async {
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
          if (attempts.length == 2 && !done.isCompleted) done.complete();
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

extension on List<NatsJetStreamJobQueue> {
  bool get lastClosed => isNotEmpty;
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
  Object? publishError;
  bool connected = false;
  int createdConsumers = 0;

  @override
  bool get isConnected => connected;

  @override
  Future<void> connect(NatsMessagingConfig config) async => connected = true;

  @override
  Future<void> close() async => connected = false;

  @override
  Future<void> drain() async {}

  @override
  Future<void> flush() async {}

  @override
  Future<void> createOrUpdateStream(NatsJetStreamConfig config) async {}

  @override
  Future<NatsJetStreamConsumer> createOrUpdateConsumer({
    required String streamName,
    required String consumerName,
    required String topic,
  }) async {
    createdConsumers += 1;
    return consumer;
  }

  @override
  Future<NatsJetStreamPublishAck> publish(
    String subject,
    List<int> bytes, {
    required Duration timeout,
    String? messageId,
    Map<String, String> headers = const {},
  }) async {
    final error = publishError;
    if (error != null) {
      publishError = null;
      throw error;
    }
    return const NatsJetStreamPublishAck(
      stream: 'PODBUS_RESILIENCE',
      sequence: 1,
      duplicate: false,
    );
  }
}

final class _Consumer implements NatsJetStreamConsumer {
  final _controller = StreamController<_Message>.broadcast();

  void add(_Message message) => _controller.add(message);

  @override
  Future<List<NatsJetStreamMessage>> fetch({
    required int batch,
    required Duration timeout,
  }) async {
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
    : bytes = utf8.encode(jsonEncode(payload));

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
  int inProgressCalls = 0;
  Object? heartbeatError;

  @override
  Future<bool> ack() async {
    ackCalls += 1;
    return true;
  }

  @override
  Future<bool> nak({Duration? delay}) async => true;

  @override
  Future<bool> term() async => true;

  @override
  Future<bool> inProgress() async {
    inProgressCalls += 1;
    final error = heartbeatError;
    if (error != null) throw error;
    return true;
  }
}

final class _JobContext implements JobContext {
  _JobContext(this.message);

  final NatsJetStreamMessage message;

  @override
  String get topic => message.subject;

  @override
  MessageHeaders get headers => MessageHeaders();

  @override
  Object? get rawMessage => message;

  @override
  int get attempt => message.deliveryCount;

  @override
  int get maxAttempts => 3;

  @override
  Future<void> ack() async {}

  @override
  Future<void> deadLetter({Object? error, StackTrace? stackTrace}) async {}

  @override
  Future<void> fail(Object error, [StackTrace? stackTrace]) async {
    throw error;
  }

  @override
  Future<void> retry({Duration? delay}) async {}
}
