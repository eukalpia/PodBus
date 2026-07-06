import 'dart:async';
import 'dart:collection';
import 'dart:convert';

import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_kafka/podbus_kafka.dart';
import 'package:podbus_kafka/src/kafka_adapter.dart';
import 'package:test/test.dart';

void main() {
  group('KafkaEventBus', () {
    test('produces enveloped events with PodBus headers', () async {
      final adapter = FakeKafkaAdapter();
      final bus = KafkaEventBus(config: _config(), adapter: adapter);

      await bus.connect();
      await bus.publish('leads.created', {
        'leadId': 7,
      }, headers: MessageHeaders(correlationId: 'corr-1'));

      final envelope = _decodeEnvelope(adapter.produced.single.bytes);
      expect(adapter.produced.single.topic, 'leads.created');
      expect(envelope['headers'], containsPair('correlationId', 'corr-1'));
      expect(envelope['contentType'], JsonMessageCodec.contentType);
      await bus.close();
    });

    test('commits an event offset after successful handling', () async {
      final adapter = FakeKafkaAdapter();
      final bus = KafkaEventBus(config: _config(), adapter: adapter);
      await bus.connect();

      final received = Completer<Map<String, Object?>>();
      final subscription = await bus.subscribe<Map<String, Object?>>(
        'leads.created',
        handler: (context, payload) async {
          expect(context.subject, 'leads.created');
          expect(context.headers.correlationId, 'corr-1');
          received.complete(payload);
        },
      );

      adapter.consumer.add(
        KafkaAdapterRecord(
          topic: 'leads.created',
          bytes: _recordBytes({
            'leadId': 7,
          }, headers: MessageHeaders(correlationId: 'corr-1')),
          key: null,
          partition: 0,
          offset: 1,
        ),
      );

      expect(await received.future.timeout(_testTimeout), {'leadId': 7});
      await adapter.consumer.committed.future.timeout(_testTimeout);

      await subscription.close();
      await bus.close();
    });

    test('does not commit a failed event without dead-letter policy', () async {
      final adapter = FakeKafkaAdapter();
      final bus = KafkaEventBus(config: _config(), adapter: adapter);
      await bus.connect();

      final failed = Completer<void>();
      final subscription = await bus.subscribe<Map<String, Object?>>(
        'leads.created',
        handler: (_, _) async {
          failed.complete();
          throw StateError('handler failed');
        },
      );

      adapter.consumer.add(
        KafkaAdapterRecord(
          topic: 'leads.created',
          bytes: _recordBytes({'leadId': 7}),
          key: null,
          partition: 0,
          offset: 1,
        ),
      );

      await failed.future.timeout(_testTimeout);
      await Future<void>.delayed(Duration(milliseconds: 50));
      expect(adapter.consumer.commitCount, 0);

      await subscription.close();
      await bus.close();
    });

    test('stops an event consumer after a failed offset', () async {
      final adapter = FakeKafkaAdapter();
      final bus = KafkaEventBus(config: _config(), adapter: adapter);
      await bus.connect();

      final failed = Completer<void>();
      final handled = <int>[];
      final subscription = await bus.subscribe<Map<String, Object?>>(
        'leads.created',
        handler: (_, payload) async {
          handled.add(payload['leadId'] as int);
          if (payload['leadId'] == 1) {
            failed.complete();
            throw StateError('handler failed');
          }
        },
      );

      adapter.consumer.add(
        KafkaAdapterRecord(
          topic: 'leads.created',
          bytes: _recordBytes({'leadId': 1}),
          key: null,
          partition: 0,
          offset: 1,
        ),
      );
      await failed.future.timeout(_testTimeout);

      adapter.consumer.add(
        KafkaAdapterRecord(
          topic: 'leads.created',
          bytes: _recordBytes({'leadId': 2}),
          key: null,
          partition: 0,
          offset: 2,
        ),
      );

      await Future<void>.delayed(const Duration(milliseconds: 300));
      expect(handled, [1]);
      expect(adapter.consumer.commitCount, 0);
      final health = await bus.healthCheck();
      expect(health.status, HealthStatus.unhealthy);
      expect(health.details['lastConsumerError'], contains('handler failed'));

      await subscription.close();
      await bus.close();
    });

    test('dead-letters a failed job and commits the source offset', () async {
      final adapter = FakeKafkaAdapter();
      final bus = KafkaEventBus(config: _config(), adapter: adapter);
      await bus.connect();

      final worker = await bus.worker<Map<String, Object?>>(
        'jobs.email',
        deadLetterPolicy: const DeadLetterPolicy(
          enabled: true,
          destination: 'jobs.email.dead',
          includeErrorDetails: true,
        ),
        handler: (_, _) async {
          throw StateError('smtp unavailable');
        },
      );

      adapter.consumer.add(
        KafkaAdapterRecord(
          topic: 'jobs.email',
          bytes: _recordBytes({'leadId': 7}),
          key: 'welcome-7',
          partition: 0,
          offset: 2,
        ),
      );

      await adapter.consumer.committed.future.timeout(_testTimeout);
      expect(adapter.produced.single.topic, 'jobs.email.dead');
      final envelope = _decodeEnvelope(adapter.produced.single.bytes);
      expect(
        envelope['headers'],
        containsPair('podbus-dead-letter-error', contains('smtp unavailable')),
      );

      await worker.close();
      await bus.close();
    });

    test(
      'stops a worker after a failed offset without dead-letter policy',
      () async {
        final adapter = FakeKafkaAdapter();
        final bus = KafkaEventBus(config: _config(), adapter: adapter);
        await bus.connect();

        final failed = Completer<void>();
        final handled = <int>[];
        final worker = await bus.worker<Map<String, Object?>>(
          'jobs.email',
          handler: (_, payload) async {
            handled.add(payload['leadId'] as int);
            if (payload['leadId'] == 1) {
              failed.complete();
              throw StateError('smtp unavailable');
            }
          },
        );

        adapter.consumer.add(
          KafkaAdapterRecord(
            topic: 'jobs.email',
            bytes: _recordBytes({'leadId': 1}),
            key: null,
            partition: 0,
            offset: 1,
          ),
        );
        await failed.future.timeout(_testTimeout);

        adapter.consumer.add(
          KafkaAdapterRecord(
            topic: 'jobs.email',
            bytes: _recordBytes({'leadId': 2}),
            key: null,
            partition: 0,
            offset: 2,
          ),
        );

        await Future<void>.delayed(const Duration(milliseconds: 300));
        expect(handled, [1]);
        expect(adapter.consumer.commitCount, 0);
        final health = await bus.healthCheck();
        expect(health.status, HealthStatus.unhealthy);
        expect(
          health.details['lastConsumerError'],
          contains('smtp unavailable'),
        );

        await worker.close();
        await bus.close();
      },
    );
  });
}

const _testTimeout = Duration(seconds: 2);

KafkaMessagingConfig _config() {
  return KafkaMessagingConfig(
    brokers: ['localhost:9092'],
    clientId: 'podbus-tests',
    groupId: 'podbus-tests',
  );
}

List<int> _recordBytes(
  Map<String, Object?> payload, {
  MessageHeaders? headers,
}) {
  final encoded = utf8.encode(jsonEncode(payload));
  return utf8.encode(
    jsonEncode({
      'headers': (headers ?? MessageHeaders()).toMap(),
      'contentType': JsonMessageCodec.contentType,
      'schemaVersion': 1,
      'payload': base64Encode(encoded),
    }),
  );
}

Map<String, Object?> _decodeEnvelope(List<int> bytes) {
  return jsonDecode(utf8.decode(bytes)) as Map<String, Object?>;
}

final class FakeKafkaAdapter implements KafkaAdapter {
  final produced = <FakeKafkaProduced>[];
  final consumer = FakeKafkaConsumer();
  var connected = false;

  @override
  bool get isConnected => connected;

  @override
  Future<void> close() async {
    connected = false;
    await consumer.close();
  }

  @override
  Future<void> connect(KafkaMessagingConfig config) async {
    connected = true;
  }

  @override
  Future<KafkaAdapterConsumer> consumerFor({
    required List<String> topics,
    required String groupId,
  }) async {
    consumer.topics = topics;
    consumer.groupId = groupId;
    return consumer;
  }

  @override
  Future<void> flush([Duration? timeout]) async {}

  @override
  Future<void> produce({
    required String topic,
    required List<int> bytes,
    String? key,
  }) async {
    produced.add(FakeKafkaProduced(topic, bytes, key));
  }
}

final class FakeKafkaConsumer implements KafkaAdapterConsumer {
  final _records = Queue<KafkaAdapterRecord>();
  Completer<void>? _available;
  var topics = <String>[];
  var groupId = '';
  var commitCount = 0;
  var committed = Completer<void>();

  void add(KafkaAdapterRecord record) {
    _records.add(record);
    _available?.complete();
    _available = null;
  }

  @override
  Future<void> close() async {}

  @override
  Future<void> commit() async {
    commitCount += 1;
    if (!committed.isCompleted) {
      committed.complete();
    }
  }

  @override
  Future<KafkaAdapterRecord?> poll(Duration timeout) async {
    if (_records.isNotEmpty) {
      return _records.removeFirst();
    }
    final available = _available ??= Completer<void>();
    try {
      await available.future.timeout(timeout);
    } on TimeoutException {
      return null;
    }
    if (_records.isEmpty) {
      return null;
    }
    return _records.removeFirst();
  }
}

final class FakeKafkaProduced {
  const FakeKafkaProduced(this.topic, this.bytes, this.key);

  final String topic;
  final List<int> bytes;
  final String? key;
}
