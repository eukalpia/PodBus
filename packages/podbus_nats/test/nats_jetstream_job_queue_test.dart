import 'dart:async';

import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_nats/podbus_nats.dart';
import 'package:podbus_nats/src/nats_jetstream_adapter.dart';
import 'package:test/test.dart';

void main() {
  group('NatsJetStreamJobQueue', () {
    test('sets up the stream and publishes durable jobs', () async {
      final adapter = FakeNatsJetStreamAdapter();
      final queue = NatsJetStreamJobQueue(
        config: _config(),
        jetStreamAdapter: adapter,
      );

      await queue.connect();
      await queue.enqueue(
        'jobs.email',
        {'leadId': 7},
        headers: MessageHeaders(correlationId: 'corr-1'),
        idempotencyKey: 'welcome-7',
      );

      expect(adapter.createdStreams.single.streamName, 'PODBUS_TESTS');
      expect(adapter.published.single.subject, 'jobs.email');
      expect(adapter.published.single.messageId, 'welcome-7');
      expect(adapter.published.single.headers['correlationId'], 'corr-1');
      expect(
        adapter.published.single.headers['podbus-content-type'],
        JsonMessageCodec.contentType,
      );
      await queue.close();
    });

    test('acks a job after successful handling', () async {
      final adapter = FakeNatsJetStreamAdapter();
      final queue = NatsJetStreamJobQueue(
        config: _config(),
        jetStreamAdapter: adapter,
      );
      await queue.connect();

      final handled = Completer<Map<String, Object?>>();
      final worker = await queue.worker<Map<String, Object?>>(
        'jobs.email',
        durableName: 'email-workers',
        handler: (context, payload) async {
          expect(context.topic, 'jobs.email');
          expect(context.attempt, 1);
          handled.complete(payload);
        },
      );

      final message = FakeNatsJetStreamMessage(
        subject: 'jobs.email',
        bytes: '{"leadId":7}'.codeUnits,
        headers: _jsonHeaders(),
      );
      adapter.consumers.single.add(message);

      expect(await handled.future.timeout(_testTimeout), {'leadId': 7});
      await message.acked.future.timeout(_testTimeout);
      expect(adapter.consumers.single.consumerName, 'email-workers');

      await worker.close();
      await queue.close();
    });

    test('naks failed jobs before max attempts is reached', () async {
      final adapter = FakeNatsJetStreamAdapter();
      final queue = NatsJetStreamJobQueue(
        config: _config(),
        jetStreamAdapter: adapter,
      );
      await queue.connect();

      final worker = await queue.worker<Map<String, Object?>>(
        'jobs.email',
        durableName: 'email-workers',
        retryPolicy: RetryPolicy(
          maxAttempts: 2,
          initialDelay: Duration.zero,
          maxDelay: Duration.zero,
        ),
        handler: (_, _) async {
          throw StateError('smtp unavailable');
        },
      );

      final message = FakeNatsJetStreamMessage(
        subject: 'jobs.email',
        bytes: '{"leadId":7}'.codeUnits,
        headers: _jsonHeaders(),
        deliveryCount: 1,
      );
      adapter.consumers.single.add(message);

      await message.nacked.future.timeout(_testTimeout);
      expect(message.ackCompleted, isFalse);
      expect(message.termCompleted, isFalse);

      await worker.close();
      await queue.close();
    });

    test(
      'publishes to dead letter and terminates after max attempts',
      () async {
        final adapter = FakeNatsJetStreamAdapter();
        final queue = NatsJetStreamJobQueue(
          config: _config(),
          jetStreamAdapter: adapter,
        );
        await queue.connect();

        final worker = await queue.worker<Map<String, Object?>>(
          'jobs.email',
          durableName: 'email-workers',
          retryPolicy: RetryPolicy(
            maxAttempts: 2,
            initialDelay: Duration.zero,
            maxDelay: Duration.zero,
          ),
          deadLetterPolicy: const DeadLetterPolicy(
            enabled: true,
            destination: 'jobs.email.dead',
            includeErrorDetails: true,
          ),
          handler: (_, _) async {
            throw StateError('smtp unavailable');
          },
        );

        final message = FakeNatsJetStreamMessage(
          subject: 'jobs.email',
          bytes: '{"leadId":7}'.codeUnits,
          headers: _jsonHeaders(),
          deliveryCount: 2,
        );
        adapter.consumers.single.add(message);

        await message.termed.future.timeout(_testTimeout);
        expect(adapter.published.single.subject, 'jobs.email.dead');
        expect(
          adapter.published.single.headers['podbus-dead-letter-error'],
          contains('smtp unavailable'),
        );

        await worker.close();
        await queue.close();
      },
    );

    test('fetches JetStream jobs in configured batches', () async {
      final adapter = FakeNatsJetStreamAdapter();
      final queue = NatsJetStreamJobQueue(
        config: _config(),
        jetStreamAdapter: adapter,
        fetchBatchSize: 8,
        fetchTimeout: const Duration(milliseconds: 10),
      );
      await queue.connect();

      final worker = await queue.worker<Map<String, Object?>>(
        'jobs.email',
        durableName: 'email-workers',
        handler: (_, _) async {},
      );

      await _waitFor(
        () => adapter.consumers.single.requestedBatches.isNotEmpty,
      );
      expect(adapter.consumers.single.requestedBatches, contains(8));

      await worker.close();
      await queue.close();
    });

    test('rejects invalid fetch batch size', () {
      expect(
        () => NatsJetStreamJobQueue(
          config: _config(),
          jetStreamAdapter: FakeNatsJetStreamAdapter(),
          fetchBatchSize: 0,
        ),
        throwsA(isA<MessagingConfigurationException>()),
      );
    });
  });
}

const _testTimeout = Duration(seconds: 2);

Future<void> _waitFor(bool Function() condition) async {
  final deadline = DateTime.now().add(_testTimeout);
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) {
      throw TimeoutException('Condition was not met.', _testTimeout);
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}

NatsMessagingConfig _config() {
  return NatsMessagingConfig(
    servers: [Uri.parse('nats://localhost:4222')],
    jetStream: const NatsJetStreamConfig(
      enabled: true,
      streamName: 'PODBUS_TESTS',
      subjects: ['jobs.>'],
      storage: NatsJetStreamStorage.memory,
    ),
  );
}

Map<String, String> _jsonHeaders() {
  return {
    'podbus-content-type': JsonMessageCodec.contentType,
    'podbus-schema-version': '1',
  };
}

final class FakeNatsJetStreamAdapter implements NatsJetStreamAdapter {
  final createdStreams = <NatsJetStreamConfig>[];
  final published = <FakeJetStreamPublish>[];
  final consumers = <FakeNatsJetStreamConsumer>[];
  var connected = false;

  @override
  bool get isConnected => connected;

  @override
  Future<void> close() async {
    connected = false;
  }

  @override
  Future<void> connect(NatsMessagingConfig config) async {
    connected = true;
  }

  @override
  Future<NatsJetStreamConsumer> createOrUpdateConsumer({
    required String streamName,
    required String consumerName,
    required String topic,
  }) async {
    final consumer = FakeNatsJetStreamConsumer(
      streamName: streamName,
      consumerName: consumerName,
      topic: topic,
    );
    consumers.add(consumer);
    return consumer;
  }

  @override
  Future<void> createOrUpdateStream(NatsJetStreamConfig config) async {
    createdStreams.add(config);
  }

  @override
  Future<void> drain() async {}

  @override
  Future<void> flush() async {}

  @override
  Future<NatsJetStreamPublishAck> publish(
    String subject,
    List<int> bytes, {
    required Duration timeout,
    String? messageId,
    Map<String, String> headers = const {},
  }) async {
    published.add(FakeJetStreamPublish(subject, bytes, headers, messageId));
    return const NatsJetStreamPublishAck(
      stream: 'PODBUS_TESTS',
      sequence: 1,
      duplicate: false,
    );
  }
}

final class FakeNatsJetStreamConsumer implements NatsJetStreamConsumer {
  FakeNatsJetStreamConsumer({
    required this.streamName,
    required this.consumerName,
    required this.topic,
  });

  final String streamName;
  final String consumerName;
  final String topic;
  final requestedBatches = <int>[];
  final _messages = StreamController<FakeNatsJetStreamMessage>();

  void add(FakeNatsJetStreamMessage message) {
    _messages.add(message);
  }

  @override
  Future<List<NatsJetStreamMessage>> fetch({
    required int batch,
    required Duration timeout,
  }) async {
    requestedBatches.add(batch);
    final messages = <NatsJetStreamMessage>[];
    for (var i = 0; i < batch; i += 1) {
      try {
        messages.add(await _messages.stream.first.timeout(timeout));
      } on TimeoutException {
        break;
      }
    }
    return messages;
  }
}

final class FakeNatsJetStreamMessage implements NatsJetStreamMessage {
  FakeNatsJetStreamMessage({
    required this.subject,
    required this.bytes,
    required this.headers,
    this.deliveryCount = 1,
    this.streamSequence,
    this.consumerSequence,
  });

  @override
  final String subject;

  @override
  final List<int> bytes;

  @override
  final Map<String, String> headers;

  @override
  final int deliveryCount;

  @override
  final int? streamSequence;

  @override
  final int? consumerSequence;

  final acked = Completer<void>();
  final nacked = Completer<void>();
  final termed = Completer<void>();

  bool get ackCompleted => acked.isCompleted;

  bool get termCompleted => termed.isCompleted;

  @override
  Future<bool> ack() async {
    acked.complete();
    return true;
  }

  @override
  Future<bool> inProgress() async => true;

  @override
  Future<bool> nak({Duration? delay}) async {
    nacked.complete();
    return true;
  }

  @override
  Future<bool> term() async {
    termed.complete();
    return true;
  }
}

final class FakeJetStreamPublish {
  const FakeJetStreamPublish(
    this.subject,
    this.bytes,
    this.headers,
    this.messageId,
  );

  final String subject;
  final List<int> bytes;
  final Map<String, String> headers;
  final String? messageId;
}
