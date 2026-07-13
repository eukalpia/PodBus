import 'dart:async';

import 'package:podbus_core/podbus_core.dart';
import 'package:test/test.dart';

void main() {
  group('resilient shutdown cleanup', () {
    test('message bus closes delegate when subscription close hangs', () async {
      final delegate = _HangingMessageBus();
      final bus = ResilientMessageBus(
        factory: () => delegate,
        policy: const ReconnectPolicy(healthCheckInterval: null),
      );
      await bus.connect();
      await bus.subscribe<Object?>('events', handler: (_, _) async {});

      await expectLater(
        bus.close(timeout: const Duration(milliseconds: 25)),
        throwsA(isA<TimeoutException>()),
      );
      expect(delegate.closeCalled, isTrue);
    });

    test('durable queue closes delegate when worker close hangs', () async {
      final delegate = _HangingDurableQueue();
      final queue = ResilientDurableJobQueue(
        factory: () => delegate,
        policy: const ReconnectPolicy(healthCheckInterval: null),
      );
      await queue.connect();
      await queue.worker<Object?>('jobs', handler: (_, _) async {});

      await expectLater(
        queue.close(timeout: const Duration(milliseconds: 25)),
        throwsA(isA<TimeoutException>()),
      );
      expect(delegate.closeCalled, isTrue);
    });
  });
}

final class _HangingMessageBus implements MessageBus {
  bool closeCalled = false;

  @override
  MessagingCapabilities get capabilities => MessagingCapabilities.none;

  @override
  Future<void> connect() async {}

  @override
  Future<void> close({Duration? timeout}) async {
    closeCalled = true;
  }

  @override
  Future<HealthCheckResult> healthCheck() async => HealthCheckResult.healthy();

  @override
  Future<void> publish<T>(
    String subject,
    T payload, {
    MessageHeaders? headers,
  }) async {}

  @override
  Future<TResponse> request<TRequest, TResponse>(
    String subject,
    TRequest payload, {
    MessageHeaders? headers,
    Duration? timeout,
  }) {
    throw UnimplementedError();
  }

  @override
  Future<Subscription> subscribe<T>(
    String subject, {
    String? queueGroup,
    int concurrency = 1,
    required MessageHandler<T> handler,
  }) async => _HangingSubscription();
}

final class _HangingSubscription implements Subscription {
  @override
  Future<void> close() => Completer<void>().future;
}

final class _HangingDurableQueue implements DurableJobQueue {
  bool closeCalled = false;

  @override
  MessagingCapabilities get capabilities => MessagingCapabilities.none;

  @override
  Future<void> connect() async {}

  @override
  Future<void> close({Duration? timeout}) async {
    closeCalled = true;
  }

  @override
  Future<void> enqueue<T>(
    String topic,
    T payload, {
    MessageHeaders? headers,
    String? idempotencyKey,
    DateTime? runAt,
    RetryPolicy? retryPolicy,
  }) async {}

  @override
  Future<HealthCheckResult> healthCheck() async => HealthCheckResult.healthy();

  @override
  Future<Worker> worker<T>(
    String topic, {
    String? queueGroup,
    String? durableName,
    int concurrency = 1,
    RetryPolicy? retryPolicy,
    DeadLetterPolicy? deadLetterPolicy,
    required JobHandler<T> handler,
  }) async => _HangingWorker();
}

final class _HangingWorker implements Worker {
  @override
  Future<void> close() => Completer<void>().future;
}
