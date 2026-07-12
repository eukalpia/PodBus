import 'dart:async';
import 'dart:convert';

import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_rabbitmq/podbus_rabbitmq.dart';
import 'package:podbus_rabbitmq/src/rabbitmq_adapter.dart';
import 'package:test/test.dart';

void main() {
  group('RabbitMQ resilience', () {
    test('retry publish is confirmed before source ack', () async {
      final adapter = _Adapter();
      final gate = Completer<void>();
      adapter.publishGate = gate.future;
      final bus = RabbitMqMessageBus(config: _config(), adapter: adapter);
      await bus.connect();

      final worker = await bus.worker<Map<String, Object?>>(
        'jobs.retry',
        durableName: 'retry-v1',
        retryPolicy: RetryPolicy(
          maxAttempts: 3,
          initialDelay: Duration.zero,
          maxDelay: Duration.zero,
        ),
        handler: (_, _) async => throw StateError('temporary'),
      );
      final delivery = _Delivery('jobs.retry', {
        'id': 1,
      }, events: adapter.events);
      adapter.consumer.add(delivery);

      await _waitFor(() => adapter.publishAttempts == 1);
      expect(delivery.ackCalls, 0);
      gate.complete();
      await _waitFor(() => delivery.ackCalls == 1);
      expect(adapter.events, ['publish:start', 'publish:confirmed', 'ack']);

      await worker.close();
      await bus.close();
    });

    test(
      'failed retry publish leaves source unacked and degrades health',
      () async {
        final adapter = _Adapter()
          ..publishError = const MessagingConnectionException(
            'connection lost',
          );
        final bus = RabbitMqMessageBus(config: _config(), adapter: adapter);
        await bus.connect();

        final worker = await bus.worker<Map<String, Object?>>(
          'jobs.retry-fail',
          durableName: 'retry-fail-v1',
          retryPolicy: RetryPolicy(
            maxAttempts: 3,
            initialDelay: Duration.zero,
            maxDelay: Duration.zero,
          ),
          handler: (_, _) async => throw StateError('temporary'),
        );
        final delivery = _Delivery('jobs.retry-fail', {'id': 1});
        adapter.consumer.add(delivery);

        await _waitFor(() => adapter.publishAttempts == 1);
        expect(delivery.ackCalls, 0);
        expect(delivery.nackCalls, 0);
        expect((await bus.healthCheck()).status, HealthStatus.unhealthy);

        adapter.publishError = null;
        await worker.close();
        await bus.close();
      },
    );

    test('dead-letter publish is confirmed before source ack', () async {
      final adapter = _Adapter();
      final gate = Completer<void>();
      adapter.publishGate = gate.future;
      final bus = RabbitMqMessageBus(config: _config(), adapter: adapter);
      await bus.connect();

      final worker = await bus.worker<Map<String, Object?>>(
        'jobs.dead',
        durableName: 'dead-v1',
        retryPolicy: RetryPolicy(
          maxAttempts: 1,
          initialDelay: Duration.zero,
          maxDelay: Duration.zero,
        ),
        deadLetterPolicy: const DeadLetterPolicy(
          enabled: true,
          destination: 'jobs.dead.failed',
          includeOriginalPayload: false,
          includeErrorDetails: true,
        ),
        handler: (_, _) async => throw StateError('permanent'),
      );
      final delivery = _Delivery('jobs.dead', {
        'id': 2,
      }, events: adapter.events);
      adapter.consumer.add(delivery);

      await _waitFor(() => adapter.publishAttempts == 1);
      expect(delivery.ackCalls, 0);
      gate.complete();
      await _waitFor(() => delivery.ackCalls == 1);
      expect(adapter.lastPublish?.exchange, 'podbus.dead');
      expect(adapter.lastPublish?.bytes, isEmpty);
      expect(
        adapter.lastPublish?.headers[PodBusWireHeaders
            .deadLetterPayloadOmitted],
        'true',
      );

      await worker.close();
      await bus.close();
    });

    test('200-message burst stays within configured concurrency', () async {
      final adapter = _Adapter();
      final bus = RabbitMqMessageBus(
        config: _config(prefetchCount: 16),
        adapter: adapter,
      );
      await bus.connect();

      const concurrency = 8;
      var active = 0;
      var maxActive = 0;
      var completed = 0;
      final done = Completer<void>();
      final worker = await bus.worker<Map<String, Object?>>(
        'jobs.burst',
        durableName: 'burst-v1',
        concurrency: concurrency,
        handler: (_, _) async {
          active += 1;
          if (active > maxActive) maxActive = active;
          await Future<void>.delayed(const Duration(milliseconds: 5));
          active -= 1;
          completed += 1;
          if (completed == 200 && !done.isCompleted) done.complete();
        },
      );
      final deliveries = [
        for (var index = 0; index < 200; index += 1)
          _Delivery('jobs.burst', {'id': index}),
      ];
      for (final delivery in deliveries) {
        adapter.consumer.add(delivery);
      }

      await done.future.timeout(const Duration(seconds: 5));
      expect(maxActive, lessThanOrEqualTo(concurrency));
      expect(deliveries.every((delivery) => delivery.ackCalls == 1), isTrue);
      expect(adapter.prefetchCount, 16);

      await worker.close();
      await bus.close();
    });

    test('shutdown requeues buffered work and drains active handler', () async {
      final adapter = _Adapter();
      final bus = RabbitMqMessageBus(
        config: _config(prefetchCount: 20),
        adapter: adapter,
        messagingConfig: MessagingConfig(
          shutdownTimeout: const Duration(seconds: 2),
        ),
      );
      await bus.connect();

      final started = Completer<void>();
      final release = Completer<void>();
      await bus.worker<Map<String, Object?>>(
        'jobs.shutdown',
        durableName: 'shutdown-v1',
        concurrency: 1,
        handler: (_, _) async {
          if (!started.isCompleted) started.complete();
          await release.future;
        },
      );
      final active = _Delivery('jobs.shutdown', {'id': 0});
      final buffered = [
        for (var index = 1; index <= 10; index += 1)
          _Delivery('jobs.shutdown', {'id': index}),
      ];
      adapter.consumer.add(active);
      for (final delivery in buffered) {
        adapter.consumer.add(delivery);
      }
      await started.future.timeout(_timeout);
      // Allow the asynchronous consumer stream to place deliveries into the
      // worker's pending queue before shutdown begins.
      await Future<void>.delayed(const Duration(milliseconds: 25));

      var closed = false;
      final close = bus.close().then((_) => closed = true);
      await Future<void>.delayed(const Duration(milliseconds: 30));
      expect(closed, isFalse);
      await _waitFor(() => buffered.every((delivery) => delivery.requeued));
      release.complete();
      await close.timeout(_timeout);
      expect(active.ackCalls, 1);
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

RabbitMqMessagingConfig _config({int prefetchCount = 10}) =>
    RabbitMqMessagingConfig(
      uri: Uri.parse('amqp://guest:guest@localhost:5672/%2f'),
      exchange: 'podbus.events',
      deadLetterExchange: 'podbus.dead',
      retryExchange: 'podbus.retry',
      prefetchCount: prefetchCount,
      useBrokerRetryQueues: false,
    );

final class _Adapter implements RabbitMqAdapter {
  final consumer = _Consumer();
  final events = <String>[];
  bool connected = false;
  int prefetchCount = 0;
  int publishAttempts = 0;
  Object? publishError;
  Future<void>? publishGate;
  _Publish? lastPublish;

  @override
  bool get isConnected => connected;

  @override
  Future<void> connect(RabbitMqMessagingConfig config) async =>
      connected = true;

  @override
  Future<void> close() async {
    connected = false;
    await consumer.close();
  }

  @override
  Future<void> declareExchange({
    required String name,
    required bool durable,
  }) async {}

  @override
  Future<void> declareQueue({
    required String name,
    required bool durable,
    bool exclusive = false,
    bool autoDelete = false,
    Map<String, Object?> arguments = const {},
  }) async {}

  @override
  Future<void> bindQueue({
    required String queue,
    required String exchange,
    required String routingKey,
  }) async {}

  @override
  Future<void> setPrefetchCount(int count) async => prefetchCount = count;

  @override
  Future<RabbitMqConsumer> consume({
    required String queue,
    required bool noAck,
  }) async => consumer;

  @override
  Future<void> publish({
    required String exchange,
    required String routingKey,
    required List<int> bytes,
    required Map<String, String> headers,
    required bool persistent,
  }) async {
    publishAttempts += 1;
    events.add('publish:start');
    final error = publishError;
    if (error != null) throw error;
    final gate = publishGate;
    if (gate != null) await gate;
    lastPublish = _Publish(exchange, routingKey, bytes, headers);
    events.add('publish:confirmed');
  }
}

final class _Consumer implements RabbitMqConsumer {
  final controller = StreamController<RabbitMqDelivery>();
  bool closed = false;

  void add(RabbitMqDelivery delivery) => controller.add(delivery);

  @override
  Stream<RabbitMqDelivery> get deliveries => controller.stream;

  @override
  Future<void> close() async {
    if (closed) return;
    closed = true;
    await controller.close();
  }
}

final class _Delivery implements RabbitMqDelivery {
  _Delivery(
    this.routingKey,
    Map<String, Object?> payload, {
    int attempt = 1,
    this.events,
  }) : bytes = utf8.encode(jsonEncode(payload)),
       headers = {
         PodBusWireHeaders.contentType: JsonMessageCodec.contentType,
         PodBusWireHeaders.schemaVersion: '1',
         'attempt': attempt.toString(),
       };

  @override
  final String routingKey;

  @override
  final List<int> bytes;

  @override
  final Map<String, String> headers;

  final List<String>? events;
  int ackCalls = 0;
  int nackCalls = 0;
  bool requeued = false;

  @override
  Future<void> ack() async {
    ackCalls += 1;
    events?.add('ack');
  }

  @override
  Future<void> nack({required bool requeue}) async {
    nackCalls += 1;
    requeued = requeue;
  }
}

final class _Publish {
  const _Publish(this.exchange, this.routingKey, this.bytes, this.headers);

  final String exchange;
  final String routingKey;
  final List<int> bytes;
  final Map<String, String> headers;
}
