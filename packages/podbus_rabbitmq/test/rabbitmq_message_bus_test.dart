import 'dart:async';

import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_rabbitmq/podbus_rabbitmq.dart';
import 'package:podbus_rabbitmq/src/rabbitmq_adapter.dart';
import 'package:test/test.dart';

void main() {
  group('RabbitMqMessageBus', () {
    test('declares exchanges and publishes persistent messages', () async {
      final adapter = FakeRabbitMqAdapter();
      final bus = RabbitMqMessageBus(config: _config(), adapter: adapter);

      await bus.connect();
      await bus.publish('leads.created', {
        'leadId': 7,
      }, headers: MessageHeaders(correlationId: 'corr-1'));

      expect(adapter.declaredExchanges, [
        ('podbus.events', true),
        ('podbus.dead', true),
        ('podbus.events.retry', true),
      ]);
      expect(adapter.published.single.exchange, 'podbus.events');
      expect(adapter.published.single.routingKey, 'leads.created');
      expect(adapter.published.single.persistent, isTrue);
      expect(adapter.published.single.headers['correlationId'], 'corr-1');
      expect(
        adapter.published.single.headers['podbus-content-type'],
        JsonMessageCodec.contentType,
      );
      await bus.close();
    });

    test('binds a queue and acknowledges consumed events', () async {
      final adapter = FakeRabbitMqAdapter();
      final bus = RabbitMqMessageBus(config: _config(), adapter: adapter);
      await bus.connect();

      final received = Completer<Map<String, Object?>>();
      final subscription = await bus.subscribe<Map<String, Object?>>(
        'leads.created',
        queueGroup: 'lead-workers',
        handler: (context, payload) async {
          expect(context.subject, 'leads.created');
          received.complete(payload);
        },
      );

      final delivery = FakeRabbitMqDelivery(
        routingKey: 'leads.created',
        bytes: '{"leadId":7}'.codeUnits,
        headers: _jsonHeaders(),
      );
      adapter.consumers.single.add(delivery);

      expect(await received.future.timeout(_testTimeout), {'leadId': 7});
      await delivery.acked.future.timeout(_testTimeout);
      expect(adapter.prefetchCounts.single, 10);
      expect(adapter.bindings.single.routingKey, 'leads.created');

      await subscription.close();
      await bus.close();
    });

    test('nacks events with malformed attempt headers', () async {
      final adapter = FakeRabbitMqAdapter();
      final bus = RabbitMqMessageBus(config: _config(), adapter: adapter);
      await bus.connect();

      var handled = false;
      final subscription = await bus.subscribe<Map<String, Object?>>(
        'leads.created',
        queueGroup: 'lead-workers',
        handler: (_, _) async {
          handled = true;
        },
      );

      final delivery = FakeRabbitMqDelivery(
        routingKey: 'leads.created',
        bytes: '{"leadId":7}'.codeUnits,
        headers: {..._jsonHeaders(), 'attempt': 'not-an-int'},
      );
      adapter.consumers.single.add(delivery);

      expect(await delivery.nacked.future.timeout(_testTimeout), isFalse);
      expect(handled, isFalse);
      expect(delivery.isAcked, isFalse);

      await subscription.close();
      await bus.close();
    });

    test(
      'retries failed jobs by republishing with incremented attempt',
      () async {
        final adapter = FakeRabbitMqAdapter();
        final bus = RabbitMqMessageBus(config: _config(), adapter: adapter);
        await bus.connect();

        final worker = await bus.worker<Map<String, Object?>>(
          'jobs.email',
          retryPolicy: RetryPolicy(
            maxAttempts: 2,
            initialDelay: Duration.zero,
            maxDelay: Duration.zero,
          ),
          handler: (_, _) async {
            throw StateError('smtp unavailable');
          },
        );

        final delivery = FakeRabbitMqDelivery(
          routingKey: 'jobs.email',
          bytes: '{"leadId":7}'.codeUnits,
          headers: _jsonHeaders(attempt: 1),
        );
        adapter.consumers.single.add(delivery);

        await delivery.acked.future.timeout(_testTimeout);
        expect(adapter.published.single.exchange, 'podbus.events');
        expect(adapter.published.single.routingKey, 'jobs.email');
        expect(adapter.published.single.headers['attempt'], '2');

        await worker.close();
        await bus.close();
      },
    );

    test(
      'publishes failed jobs to the dead-letter exchange at max attempts',
      () async {
        final adapter = FakeRabbitMqAdapter();
        final bus = RabbitMqMessageBus(config: _config(), adapter: adapter);
        await bus.connect();

        final worker = await bus.worker<Map<String, Object?>>(
          'jobs.email',
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

        final delivery = FakeRabbitMqDelivery(
          routingKey: 'jobs.email',
          bytes: '{"leadId":7}'.codeUnits,
          headers: _jsonHeaders(attempt: 2),
        );
        adapter.consumers.single.add(delivery);

        await delivery.acked.future.timeout(_testTimeout);
        expect(adapter.published.single.exchange, 'podbus.dead');
        expect(adapter.published.single.routingKey, 'jobs.email.dead');
        expect(
          adapter.published.single.headers['podbus-dead-letter-error'],
          contains('smtp unavailable'),
        );

        await worker.close();
        await bus.close();
      },
    );

    test(
      'does not ack failed jobs when dead-letter publishing fails',
      () async {
        final adapter = FakeRabbitMqAdapter()
          ..publishError = StateError('broker rejected publish');
        final bus = RabbitMqMessageBus(config: _config(), adapter: adapter);
        await bus.connect();

        final worker = await bus.worker<Map<String, Object?>>(
          'jobs.email',
          retryPolicy: RetryPolicy(
            maxAttempts: 1,
            initialDelay: Duration.zero,
            maxDelay: Duration.zero,
          ),
          deadLetterPolicy: const DeadLetterPolicy(
            enabled: true,
            destination: 'jobs.email.dead',
          ),
          handler: (_, _) async {
            throw StateError('smtp unavailable');
          },
        );

        final delivery = FakeRabbitMqDelivery(
          routingKey: 'jobs.email',
          bytes: '{"leadId":7}'.codeUnits,
          headers: _jsonHeaders(),
        );
        adapter.consumers.single.add(delivery);

        await _waitFor(() => adapter.publishAttempts == 1);
        expect(delivery.isAcked, isFalse);
        expect(delivery.isNacked, isFalse);
        final health = await bus.healthCheck();
        expect(health.status, HealthStatus.unhealthy);
        expect(
          health.details['lastWorkerError'],
          contains('broker rejected publish'),
        );

        await worker.close();
        await bus.close();
      },
    );

    test('dead-letters jobs with malformed attempt headers', () async {
      final adapter = FakeRabbitMqAdapter();
      final bus = RabbitMqMessageBus(config: _config(), adapter: adapter);
      await bus.connect();

      var handled = false;
      final worker = await bus.worker<Map<String, Object?>>(
        'jobs.email',
        deadLetterPolicy: const DeadLetterPolicy(
          enabled: true,
          destination: 'jobs.email.dead',
          includeErrorDetails: true,
        ),
        handler: (_, _) async {
          handled = true;
        },
      );

      final delivery = FakeRabbitMqDelivery(
        routingKey: 'jobs.email',
        bytes: '{"leadId":7}'.codeUnits,
        headers: {..._jsonHeaders(), 'attempt': 'not-an-int'},
      );
      adapter.consumers.single.add(delivery);

      await delivery.acked.future.timeout(_testTimeout);
      expect(handled, isFalse);
      expect(adapter.published.single.exchange, 'podbus.dead');
      expect(adapter.published.single.routingKey, 'jobs.email.dead');
      expect(
        adapter.published.single.headers['podbus-dead-letter-error'],
        contains('not-an-int'),
      );

      await worker.close();
      await bus.close();
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

RabbitMqMessagingConfig _config() {
  return RabbitMqMessagingConfig(
    uri: Uri.parse('amqp://guest:guest@localhost:5672'),
    exchange: 'podbus.events',
    deadLetterExchange: 'podbus.dead',
  );
}

Map<String, String> _jsonHeaders({int attempt = 1}) {
  return {
    'attempt': attempt.toString(),
    'podbus-content-type': JsonMessageCodec.contentType,
    'podbus-schema-version': '1',
  };
}

final class FakeRabbitMqAdapter implements RabbitMqAdapter {
  final declaredExchanges = <(String, bool)>[];
  final declaredQueues = <FakeQueueDeclaration>[];
  final bindings = <FakeBinding>[];
  final prefetchCounts = <int>[];
  final consumers = <FakeRabbitMqConsumer>[];
  final published = <FakeRabbitMqPublish>[];
  Object? publishError;
  var publishAttempts = 0;
  var connected = false;

  @override
  bool get isConnected => connected;

  @override
  Future<void> bindQueue({
    required String queue,
    required String exchange,
    required String routingKey,
  }) async {
    bindings.add(FakeBinding(queue, exchange, routingKey));
  }

  @override
  Future<void> close() async {
    connected = false;
  }

  @override
  Future<void> connect(RabbitMqMessagingConfig config) async {
    connected = true;
  }

  @override
  Future<RabbitMqConsumer> consume({
    required String queue,
    required bool noAck,
  }) async {
    final consumer = FakeRabbitMqConsumer(queue, noAck);
    consumers.add(consumer);
    return consumer;
  }

  @override
  Future<void> declareExchange({
    required String name,
    required bool durable,
  }) async {
    declaredExchanges.add((name, durable));
  }

  @override
  Future<void> declareQueue({
    required String name,
    required bool durable,
    bool exclusive = false,
    bool autoDelete = false,
    Map<String, Object?> arguments = const {},
  }) async {
    declaredQueues.add(
      FakeQueueDeclaration(name, durable, exclusive, autoDelete, arguments),
    );
  }

  @override
  Future<void> publish({
    required String exchange,
    required String routingKey,
    required List<int> bytes,
    required Map<String, String> headers,
    required bool persistent,
  }) async {
    publishAttempts += 1;
    final error = publishError;
    if (error != null) {
      throw error;
    }
    published.add(
      FakeRabbitMqPublish(exchange, routingKey, bytes, headers, persistent),
    );
  }

  @override
  Future<void> setPrefetchCount(int count) async {
    prefetchCounts.add(count);
  }
}

final class FakeRabbitMqConsumer implements RabbitMqConsumer {
  FakeRabbitMqConsumer(this.queue, this.noAck);

  final String queue;
  final bool noAck;
  final controller = StreamController<RabbitMqDelivery>.broadcast();

  @override
  Stream<RabbitMqDelivery> get deliveries => controller.stream;

  void add(RabbitMqDelivery delivery) {
    controller.add(delivery);
  }

  @override
  Future<void> close() async {
    await controller.close();
  }
}

final class FakeRabbitMqDelivery implements RabbitMqDelivery {
  FakeRabbitMqDelivery({
    required this.routingKey,
    required this.bytes,
    required this.headers,
  });

  @override
  final String routingKey;

  @override
  final List<int> bytes;

  @override
  final Map<String, String> headers;

  final acked = Completer<void>();
  final nacked = Completer<bool>();

  bool get isAcked => acked.isCompleted;

  bool get isNacked => nacked.isCompleted;

  @override
  Future<void> ack() async {
    acked.complete();
  }

  @override
  Future<void> nack({required bool requeue}) async {
    nacked.complete(requeue);
  }
}

final class FakeQueueDeclaration {
  const FakeQueueDeclaration(
    this.name,
    this.durable,
    this.exclusive,
    this.autoDelete,
    this.arguments,
  );

  final String name;
  final bool durable;
  final bool exclusive;
  final bool autoDelete;
  final Map<String, Object?> arguments;
}

final class FakeBinding {
  const FakeBinding(this.queue, this.exchange, this.routingKey);

  final String queue;
  final String exchange;
  final String routingKey;
}

final class FakeRabbitMqPublish {
  const FakeRabbitMqPublish(
    this.exchange,
    this.routingKey,
    this.bytes,
    this.headers,
    this.persistent,
  );

  final String exchange;
  final String routingKey;
  final List<int> bytes;
  final Map<String, String> headers;
  final bool persistent;
}
