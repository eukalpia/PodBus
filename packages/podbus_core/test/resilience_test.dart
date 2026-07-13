import 'dart:async';

import 'package:podbus_core/podbus_core.dart';
import 'package:test/test.dart';

void main() {
  group('ReconnectPolicy', () {
    test('caps exponential backoff', () {
      const policy = ReconnectPolicy(
        initialDelay: Duration(milliseconds: 100),
        maxDelay: Duration(milliseconds: 250),
        backoffMultiplier: 2,
        jitter: 0,
      );

      expect(policy.delayForAttempt(1), const Duration(milliseconds: 100));
      expect(policy.delayForAttempt(2), const Duration(milliseconds: 200));
      expect(policy.delayForAttempt(3), const Duration(milliseconds: 250));
      expect(policy.delayForAttempt(10), const Duration(milliseconds: 250));
    });
  });

  group('ResilientMessageBus', () {
    test('coalesces concurrent initial connect calls', () async {
      final release = Completer<void>();
      var factoryCalls = 0;
      final bus = ResilientMessageBus(
        factory: () {
          factoryCalls += 1;
          return _FakeMessageBus(connectGate: release.future);
        },
        policy: const ReconnectPolicy(healthCheckInterval: null),
      );

      final first = bus.connect();
      final second = bus.connect();
      await Future<void>.delayed(Duration.zero);
      expect(factoryCalls, 1);

      release.complete();
      await Future.wait([first, second]);
      expect(factoryCalls, 1);
      await bus.close();
    });

    test(
      'close cancels an in-flight connect without leaking the delegate',
      () async {
        final release = Completer<void>();
        final delegate = _FakeMessageBus(connectGate: release.future);
        final bus = ResilientMessageBus(
          factory: () => delegate,
          policy: const ReconnectPolicy(healthCheckInterval: null),
        );

        final connecting = bus.connect();
        await Future<void>.delayed(Duration.zero);
        await bus.close(timeout: const Duration(milliseconds: 20));
        release.complete();

        await expectLater(
          connecting,
          throwsA(isA<MessagingConnectionException>()),
        );
        expect(delegate.closed, isTrue);
      },
    );

    test(
      'proactively recovers unhealthy delegates and restores subscriptions',
      () async {
        final delegates = <_FakeMessageBus>[];
        var handled = 0;
        final bus = ResilientMessageBus(
          factory: () {
            final delegate = _FakeMessageBus();
            delegates.add(delegate);
            return delegate;
          },
          policy: const ReconnectPolicy(
            initialDelay: Duration.zero,
            maxDelay: Duration.zero,
            jitter: 0,
            healthCheckInterval: Duration(milliseconds: 10),
            healthCheckTimeout: Duration(milliseconds: 50),
          ),
        );

        await bus.connect();
        await bus.subscribe<int>(
          'events.health',
          handler: (_, _) async => handled += 1,
        );
        delegates.first.connected = false;

        await _waitUntil(() => delegates.length == 2);
        expect(delegates.first.closed, isTrue);
        expect(delegates.last.subscriptionCount, 1);
        await delegates.last.deliver('events.health', 1);
        expect(handled, 1);
        await bus.close();
      },
    );

    test('closes a delegate when initial connect fails', () async {
      final delegate = _FakeMessageBus(
        connectError: const MessagingConnectionException('unavailable'),
      );
      final bus = ResilientMessageBus(
        factory: () => delegate,
        policy: const ReconnectPolicy(healthCheckInterval: null),
      );

      await expectLater(
        bus.connect(),
        throwsA(isA<MessagingConnectionException>()),
      );
      expect(delegate.closed, isTrue);
    });

    test('rejects invalid reconnect durations at runtime', () {
      expect(
        () => ResilientMessageBus(
          factory: _FakeMessageBus.new,
          policy: ReconnectPolicy(
            initialDelay: const Duration(milliseconds: -1),
          ),
        ),
        throwsA(isA<MessagingConfigurationException>()),
      );
    });
    test('reconnects and retries publish after connection loss', () async {
      final delegates = <_FakeMessageBus>[];
      final bus = ResilientMessageBus(
        factory: () {
          final delegate = _FakeMessageBus();
          delegates.add(delegate);
          return delegate;
        },
        policy: const ReconnectPolicy(
          maxAttempts: 3,
          initialDelay: Duration.zero,
          maxDelay: Duration.zero,
          jitter: 0,
        ),
      );

      await bus.connect();
      delegates.single.failNextPublish = true;

      await bus.publish('events.created', {'id': 1});

      expect(delegates, hasLength(2));
      expect(delegates.first.closed, isTrue);
      expect(delegates.last.publishedSubjects, ['events.created']);
      await bus.close();
    });

    test('restores active subscriptions exactly once', () async {
      final delegates = <_FakeMessageBus>[];
      var handled = 0;
      final bus = ResilientMessageBus(
        factory: () {
          final delegate = _FakeMessageBus();
          delegates.add(delegate);
          return delegate;
        },
        policy: const ReconnectPolicy(
          initialDelay: Duration.zero,
          maxDelay: Duration.zero,
          jitter: 0,
        ),
      );

      await bus.connect();
      final subscription = await bus.subscribe<Map<String, Object?>>(
        'events.created',
        queueGroup: 'workers',
        concurrency: 4,
        handler: (_, _) async => handled += 1,
      );
      expect(delegates.single.subscriptionCount, 1);

      delegates.single.failNextPublish = true;
      await bus.publish('events.created', {'id': 2});

      expect(delegates, hasLength(2));
      expect(delegates.last.subscriptionCount, 1);
      await delegates.last.deliver('events.created', {'id': 2});
      expect(handled, 1);

      await subscription.close();
      expect(delegates.last.closedSubscriptions, 1);
      await bus.close();
    });

    test('coalesces concurrent recovery into one replacement', () async {
      final delegates = <_FakeMessageBus>[];
      final releaseConnect = Completer<void>();
      var factoryCalls = 0;
      final bus = ResilientMessageBus(
        factory: () {
          factoryCalls += 1;
          final delegate = _FakeMessageBus(
            connectGate: factoryCalls == 2 ? releaseConnect.future : null,
          );
          delegates.add(delegate);
          return delegate;
        },
        policy: const ReconnectPolicy(
          initialDelay: Duration.zero,
          maxDelay: Duration.zero,
          jitter: 0,
        ),
      );

      await bus.connect();
      delegates.single.failAllPublishes = true;
      final first = bus.publish('events.a', 1);
      final second = bus.publish('events.b', 2);
      await Future<void>.delayed(Duration.zero);
      releaseConnect.complete();
      await Future.wait([first, second]);

      expect(factoryCalls, 2);
      expect(
        delegates.last.publishedSubjects,
        containsAll(['events.a', 'events.b']),
      );
      await bus.close();
    });

    test(
      'closes a failed replacement when subscription restoration fails',
      () async {
        final delegates = <_FakeMessageBus>[];
        final bus = ResilientMessageBus(
          factory: () {
            final delegate = _FakeMessageBus(
              subscribeError: delegates.isEmpty
                  ? null
                  : StateError('subscription restore failed'),
            );
            delegates.add(delegate);
            return delegate;
          },
          policy: const ReconnectPolicy(
            maxAttempts: 1,
            initialDelay: Duration.zero,
            maxDelay: Duration.zero,
            jitter: 0,
            healthCheckInterval: null,
          ),
        );

        await bus.connect();
        await bus.subscribe<int>('events.restore', handler: (_, _) async {});
        delegates.first.failNextPublish = true;

        await expectLater(
          bus.publish('events.restore', 1),
          throwsA(isA<MessagingConnectionException>()),
        );
        expect(delegates, hasLength(2));
        expect(delegates.last.closed, isTrue);
        await bus.close();
      },
    );

    test('does not reconnect on application errors', () async {
      final delegates = <_FakeMessageBus>[];
      final bus = ResilientMessageBus(
        factory: () {
          final delegate = _FakeMessageBus();
          delegates.add(delegate);
          return delegate;
        },
        policy: const ReconnectPolicy(
          initialDelay: Duration.zero,
          maxDelay: Duration.zero,
          jitter: 0,
        ),
      );
      await bus.connect();
      delegates.single.publishError = StateError('invalid payload');

      await expectLater(
        bus.publish('events.created', 1),
        throwsA(isA<StateError>()),
      );
      expect(delegates, hasLength(1));
      await bus.close();
    });

    test('reports degraded health while recovery is active', () async {
      final releaseConnect = Completer<void>();
      var calls = 0;
      late _FakeMessageBus first;
      final bus = ResilientMessageBus(
        factory: () {
          calls += 1;
          final delegate = _FakeMessageBus(
            connectGate: calls == 2 ? releaseConnect.future : null,
          );
          if (calls == 1) first = delegate;
          return delegate;
        },
        policy: const ReconnectPolicy(
          initialDelay: Duration.zero,
          maxDelay: Duration.zero,
          jitter: 0,
        ),
      );
      await bus.connect();
      first.failNextPublish = true;
      final publish = bus.publish('events.created', 1);
      await Future<void>.delayed(Duration.zero);

      final health = await bus.healthCheck();
      expect(health.status, HealthStatus.degraded);
      releaseConnect.complete();
      await publish;
      await bus.close();
    });

    test('ignores a health probe completed after message-bus close', () async {
      final gate = Completer<HealthCheckResult>();
      final delegates = <_FakeMessageBus>[];
      final bus = ResilientMessageBus(
        factory: () {
          final value = _FakeMessageBus(
            healthGate: delegates.isEmpty ? gate.future : null,
          );
          delegates.add(value);
          return value;
        },
        policy: const ReconnectPolicy(
          initialDelay: Duration.zero,
          maxDelay: Duration.zero,
          jitter: 0,
          healthCheckInterval: Duration(milliseconds: 1),
          healthCheckTimeout: Duration(seconds: 1),
        ),
      );

      await bus.connect();
      await _waitUntil(() => delegates.first.healthChecks > 0);
      await bus.close(timeout: const Duration(milliseconds: 100));
      gate.complete(HealthCheckResult.unhealthy(message: 'late'));
      await Future<void>.delayed(const Duration(milliseconds: 30));

      expect(delegates, hasLength(1));
      await bus.close();
    });
  });

  group('ResilientDurableJobQueue', () {
    test(
      'ignores a health probe completed after durable-queue close',
      () async {
        final gate = Completer<HealthCheckResult>();
        final delegates = <_FakeDurableQueue>[];
        final queue = ResilientDurableJobQueue(
          factory: () {
            final value = _FakeDurableQueue(
              healthGate: delegates.isEmpty ? gate.future : null,
            );
            delegates.add(value);
            return value;
          },
          policy: const ReconnectPolicy(
            initialDelay: Duration.zero,
            maxDelay: Duration.zero,
            jitter: 0,
            healthCheckInterval: Duration(milliseconds: 1),
            healthCheckTimeout: Duration(seconds: 1),
          ),
        );

        await queue.connect();
        await _waitUntil(() => delegates.first.healthChecks > 0);
        await queue.close(timeout: const Duration(milliseconds: 100));
        gate.complete(HealthCheckResult.unhealthy(message: 'late'));
        await Future<void>.delayed(const Duration(milliseconds: 30));

        expect(delegates, hasLength(1));
        await queue.close();
      },
    );

    test(
      'proactively recovers unhealthy delegates and restores workers',
      () async {
        final delegates = <_FakeDurableQueue>[];
        var handled = 0;
        final queue = ResilientDurableJobQueue(
          factory: () {
            final delegate = _FakeDurableQueue();
            delegates.add(delegate);
            return delegate;
          },
          policy: const ReconnectPolicy(
            initialDelay: Duration.zero,
            maxDelay: Duration.zero,
            jitter: 0,
            healthCheckInterval: Duration(milliseconds: 10),
            healthCheckTimeout: Duration(milliseconds: 50),
          ),
        );

        await queue.connect();
        await queue.worker<int>(
          'jobs.health',
          handler: (_, _) async => handled += 1,
        );
        delegates.first.connected = false;

        await _waitUntil(() => delegates.length == 2);
        expect(delegates.last.workerCount, 1);
        await delegates.last.deliver('jobs.health', 1);
        expect(handled, 1);
        await queue.close();
      },
    );
    test('restores durable workers before retrying enqueue', () async {
      final delegates = <_FakeDurableQueue>[];
      var handled = 0;
      final queue = ResilientDurableJobQueue(
        factory: () {
          final delegate = _FakeDurableQueue();
          delegates.add(delegate);
          return delegate;
        },
        policy: const ReconnectPolicy(
          initialDelay: Duration.zero,
          maxDelay: Duration.zero,
          jitter: 0,
        ),
      );

      await queue.connect();
      final worker = await queue.worker<Map<String, Object?>>(
        'jobs.email',
        durableName: 'email-v1',
        concurrency: 8,
        handler: (_, _) async => handled += 1,
      );
      delegates.single.failNextEnqueue = true;

      await queue.enqueue('jobs.email', {'id': 1});

      expect(delegates, hasLength(2));
      expect(delegates.last.workerCount, 1);
      await delegates.last.deliver('jobs.email', {'id': 1});
      expect(handled, 1);
      await worker.close();
      await queue.close();
    });

    test('retries failed reconnect attempts with bounded policy', () async {
      var factoryCalls = 0;
      final attempts = <int>[];
      late _FakeDurableQueue first;
      final queue = ResilientDurableJobQueue(
        factory: () {
          factoryCalls += 1;
          final delegate = _FakeDurableQueue(
            connectError: factoryCalls == 2
                ? const MessagingConnectionException('broker unavailable')
                : null,
          );
          if (factoryCalls == 1) first = delegate;
          return delegate;
        },
        policy: const ReconnectPolicy(
          maxAttempts: 3,
          initialDelay: Duration.zero,
          maxDelay: Duration.zero,
          jitter: 0,
        ),
        onReconnectAttempt: (_, attempt, _) => attempts.add(attempt),
      );

      await queue.connect();
      first.failNextEnqueue = true;
      await queue.enqueue('jobs.a', 1);

      expect(factoryCalls, 3);
      expect(attempts, [1, 2]);
      await queue.close();
    });

    test('bounds stalled delegate disposal during recovery', () async {
      final closeGate = Completer<void>();
      final delegates = <_FakeDurableQueue>[];
      final queue = ResilientDurableJobQueue(
        factory: () {
          final delegate = _FakeDurableQueue(
            closeGate: delegates.isEmpty ? closeGate.future : null,
          );
          delegates.add(delegate);
          return delegate;
        },
        policy: const ReconnectPolicy(
          maxAttempts: 3,
          initialDelay: Duration.zero,
          maxDelay: Duration.zero,
          jitter: 0,
          recoveryTimeout: Duration(seconds: 1),
          disposeTimeout: Duration(milliseconds: 20),
          healthCheckInterval: null,
        ),
      );

      await queue.connect();
      delegates.first.failNextEnqueue = true;
      await queue
          .enqueue('jobs.recovery', 1)
          .timeout(const Duration(milliseconds: 500));

      expect(delegates, hasLength(2));
      closeGate.complete();
      await queue.close();
    });

    test('clears a timed-out recovery before the next operation', () async {
      final connectGate = Completer<void>();
      final delegates = <_FakeDurableQueue>[];
      final queue = ResilientDurableJobQueue(
        factory: () {
          final delegate = _FakeDurableQueue(
            connectGate: delegates.length == 1 ? connectGate.future : null,
          );
          delegates.add(delegate);
          return delegate;
        },
        policy: const ReconnectPolicy(
          maxAttempts: 3,
          initialDelay: Duration.zero,
          maxDelay: Duration.zero,
          jitter: 0,
          recoveryTimeout: Duration(milliseconds: 40),
          disposeTimeout: Duration(milliseconds: 10),
          healthCheckInterval: null,
        ),
      );

      await queue.connect();
      delegates.first.failNextEnqueue = true;
      await expectLater(
        queue.enqueue('jobs.timeout', 1),
        throwsA(isA<MessagingTimeoutException>()),
      );

      connectGate.complete();
      await Future<void>.delayed(const Duration(milliseconds: 10));
      await queue.enqueue('jobs.timeout', 2);
      expect(delegates.length, greaterThanOrEqualTo(3));
      await queue.close();
    });

    test('close drains active proxy registrations and delegate', () async {
      final delegate = _FakeDurableQueue();
      final queue = ResilientDurableJobQueue(factory: () => delegate);
      await queue.connect();
      await queue.worker<int>('jobs.long', handler: (_, _) async {});

      await queue.close(timeout: const Duration(seconds: 1));

      expect(delegate.closedWorkers, 1);
      expect(delegate.closed, isTrue);
      await expectLater(queue.enqueue('jobs.long', 1), completes);
      await queue.close();
    });
  });
}

Future<void> _waitUntil(bool Function() predicate) async {
  final deadline = DateTime.now().add(const Duration(seconds: 2));
  while (!predicate()) {
    if (DateTime.now().isAfter(deadline)) {
      throw TimeoutException('Condition was not met.');
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}

final class _FakeMessageBus implements MessageBus {
  _FakeMessageBus({
    this.connectGate,
    this.closeGate,
    this.healthGate,
    this.connectError,
    this.subscribeError,
  });

  final Future<void>? connectGate;
  final Future<void>? closeGate;
  final Future<HealthCheckResult>? healthGate;
  final Object? connectError;
  final Object? subscribeError;
  final List<String> publishedSubjects = [];
  final List<_FakeSubscription> subscriptions = [];
  bool failNextPublish = false;
  bool failAllPublishes = false;
  Object? publishError;
  bool connected = false;
  bool closed = false;
  int closedSubscriptions = 0;
  int healthChecks = 0;

  int get subscriptionCount => subscriptions.length;

  @override
  MessagingCapabilities get capabilities => const MessagingCapabilities({
    MessagingCapability.publishSubscribe,
    MessagingCapability.requestReply,
    MessagingCapability.gracefulShutdown,
  });

  @override
  Future<void> connect() async {
    await connectGate;
    if (connectError case final error?) throw error;
    connected = true;
  }

  @override
  Future<void> close({Duration? timeout}) async {
    await closeGate;
    closed = true;
    connected = false;
    for (final subscription in subscriptions.toList()) {
      await subscription.close();
    }
  }

  @override
  Future<void> publish<T>(
    String subject,
    T payload, {
    MessageHeaders? headers,
  }) async {
    if (publishError case final error?) throw error;
    if (failAllPublishes || failNextPublish) {
      failNextPublish = false;
      throw const MessagingConnectionException('connection lost');
    }
    publishedSubjects.add(subject);
  }

  @override
  Future<TResponse> request<TRequest, TResponse>(
    String subject,
    TRequest payload, {
    MessageHeaders? headers,
    Duration? timeout,
  }) async {
    if (!connected) {
      throw const MessagingConnectionException('connection lost');
    }
    return payload as TResponse;
  }

  @override
  Future<Subscription> subscribe<T>(
    String subject, {
    String? queueGroup,
    int concurrency = 1,
    required MessageHandler<T> handler,
  }) async {
    if (subscribeError case final error?) throw error;
    final subscription = _FakeSubscription(
      onClose: () => closedSubscriptions += 1,
      subject: subject,
      deliver: (payload) => handler(_FakeMessageContext(subject), payload as T),
    );
    subscriptions.add(subscription);
    return subscription;
  }

  Future<void> deliver(String subject, Object? payload) async {
    for (final subscription in subscriptions.where(
      (item) => !item.closed && item.subject == subject,
    )) {
      await subscription.deliver(payload);
    }
  }

  @override
  Future<HealthCheckResult> healthCheck() async {
    healthChecks += 1;
    final gate = healthGate;
    if (gate != null) {
      return gate;
    }
    return connected
        ? HealthCheckResult.healthy(message: 'connected')
        : HealthCheckResult.unhealthy(message: 'disconnected');
  }
}

final class _FakeSubscription implements Subscription {
  _FakeSubscription({
    required this.onClose,
    required this.subject,
    required this.deliver,
  });

  final void Function() onClose;
  final String subject;
  final Future<void> Function(Object? payload) deliver;
  bool closed = false;

  @override
  Future<void> close() async {
    if (closed) return;
    closed = true;
    onClose();
  }
}

final class _FakeDurableQueue implements DurableJobQueue {
  _FakeDurableQueue({
    this.connectGate,
    this.closeGate,
    this.healthGate,
    this.connectError,
  });

  final Future<void>? connectGate;
  final Future<void>? closeGate;
  final Future<HealthCheckResult>? healthGate;
  final Object? connectError;
  final List<_FakeWorker> workers = [];
  bool connected = false;
  bool closed = false;
  bool failNextEnqueue = false;
  int closedWorkers = 0;
  int healthChecks = 0;

  int get workerCount => workers.length;

  @override
  MessagingCapabilities get capabilities => const MessagingCapabilities({
    MessagingCapability.durableJobs,
    MessagingCapability.retries,
    MessagingCapability.deadLettering,
    MessagingCapability.gracefulShutdown,
  });

  @override
  Future<void> connect() async {
    await connectGate;
    if (connectError case final error?) throw error;
    connected = true;
  }

  @override
  Future<void> close({Duration? timeout}) async {
    await closeGate;
    closed = true;
    connected = false;
    for (final worker in workers.toList()) {
      await worker.close();
    }
  }

  @override
  Future<void> enqueue<T>(
    String topic,
    T payload, {
    MessageHeaders? headers,
    String? idempotencyKey,
    DateTime? runAt,
    RetryPolicy? retryPolicy,
  }) async {
    if (failNextEnqueue) {
      failNextEnqueue = false;
      throw const MessagingConnectionException('connection lost');
    }
  }

  @override
  Future<Worker> worker<T>(
    String topic, {
    String? queueGroup,
    String? durableName,
    int concurrency = 1,
    RetryPolicy? retryPolicy,
    DeadLetterPolicy? deadLetterPolicy,
    required JobHandler<T> handler,
  }) async {
    final worker = _FakeWorker(
      topic: topic,
      deliver: (payload) => handler(_FakeJobContext(topic), payload as T),
      onClose: () => closedWorkers += 1,
    );
    workers.add(worker);
    return worker;
  }

  Future<void> deliver(String topic, Object? payload) async {
    for (final worker in workers.where(
      (item) => !item.closed && item.topic == topic,
    )) {
      await worker.deliver(payload);
    }
  }

  @override
  Future<HealthCheckResult> healthCheck() async {
    healthChecks += 1;
    final gate = healthGate;
    if (gate != null) {
      return gate;
    }
    return connected
        ? HealthCheckResult.healthy(message: 'connected')
        : HealthCheckResult.unhealthy(message: 'disconnected');
  }
}

final class _FakeWorker implements Worker {
  _FakeWorker({
    required this.topic,
    required this.deliver,
    required this.onClose,
  });

  final String topic;
  final Future<void> Function(Object? payload) deliver;
  final void Function() onClose;
  bool closed = false;

  @override
  Future<void> close() async {
    if (closed) return;
    closed = true;
    onClose();
  }
}

final class _FakeMessageContext implements MessageContext {
  _FakeMessageContext(this.subject);

  @override
  final String subject;

  @override
  MessageHeaders get headers => MessageHeaders();

  @override
  Object? get rawMessage => null;

  @override
  Future<void> ack() async {}

  @override
  Future<void> extendVisibility(Duration duration) async {}

  @override
  Future<void> nak({Duration? delay}) async {}

  @override
  Future<void> reply<T>(T payload, {MessageHeaders? headers}) async {}

  @override
  Future<void> terminate() async {}
}

final class _FakeJobContext implements JobContext {
  _FakeJobContext(this.topic);

  @override
  final String topic;

  @override
  int get attempt => 1;

  @override
  int get maxAttempts => 3;

  @override
  MessageHeaders get headers => MessageHeaders();

  @override
  Object? get rawMessage => null;

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
