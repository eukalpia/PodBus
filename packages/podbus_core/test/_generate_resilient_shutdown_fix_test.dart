import 'dart:io';

import 'package:test/test.dart';

void main() {
  test('generate the resilient shutdown fix for review', () async {
    final sourceFile = File('packages/podbus_core/lib/src/resilience.dart');
    var source = await sourceFile.readAsString();

    source = source.replaceFirst(_messageBusCloseBefore, _messageBusCloseAfter);
    source = source.replaceFirst(_durableQueueCloseBefore, _durableQueueCloseAfter);
    if (!source.contains(_messageBusCloseAfter) ||
        !source.contains(_durableQueueCloseAfter)) {
      fail('The resilient shutdown source anchors did not match.');
    }

    final output = File('coverage/generated/resilience.dart');
    await output.parent.create(recursive: true);
    await output.writeAsString(source);

    final regression = File('coverage/generated/resilient_shutdown_test.dart');
    await regression.writeAsString(_regressionTest);

    final formatting = await Process.run('dart', [
      'format',
      output.path,
      regression.path,
    ]);
    expect(formatting.exitCode, 0, reason: '${formatting.stdout}\n${formatting.stderr}');
  });
}

const _messageBusCloseBefore = '''  @override
  Future<void> close({Duration? timeout}) async {
    if (_closing) {
      return;
    }
    _closing = true;
    _generation += 1;
    _stopHealthMonitor();
    final effectiveTimeout = timeout ?? policy.recoveryTimeout;
    Object? failure;
    StackTrace? failureStackTrace;
    try {
      final connecting = _connecting;
      if (connecting != null) {
        try {
          await connecting.timeout(effectiveTimeout);
        } on Object {
          // A concurrent connect is cancelled or failed; cleanup continues.
        }
      }
      final recovery = _recovery;
      if (recovery != null) {
        try {
          await recovery.timeout(effectiveTimeout);
        } on Object {
          // Shutdown cancels recovery; cleanup continues with the delegate.
        }
      }
      await Future.wait([
        for (final registration in _registrations.toList())
          registration.close(remove: false),
      ]).timeout(effectiveTimeout);
      _registrations.clear();
      final delegate = _delegate;
      _delegate = null;
      if (delegate != null) {
        await delegate
            .close(timeout: effectiveTimeout)
            .timeout(effectiveTimeout);
      }
    } on Object catch (error, stackTrace) {
      failure = error;
      failureStackTrace = stackTrace;
    } finally {
      _closing = false;
    }
    if (failure != null) {
      Error.throwWithStackTrace(failure, failureStackTrace!);
    }
  }
''';

const _messageBusCloseAfter = '''  @override
  Future<void> close({Duration? timeout}) async {
    if (_closing) {
      return;
    }
    _closing = true;
    _generation += 1;
    _stopHealthMonitor();
    final effectiveTimeout = timeout ?? policy.recoveryTimeout;
    final deadline = DateTime.now().add(effectiveTimeout);
    Object? failure;
    StackTrace? failureStackTrace;

    void captureFailure(Object error, StackTrace stackTrace) {
      failure ??= error;
      failureStackTrace ??= stackTrace;
    }

    try {
      await Future.wait([
        if (_connecting case final connecting?)
          _ignoreUntilDeadline(connecting, deadline),
        if (_recovery case final recovery?)
          _ignoreUntilDeadline(recovery, deadline),
      ]);

      final registrations = _registrations.toList();
      _registrations.clear();
      final delegate = _delegate;
      _delegate = null;
      final cleanup = Future.wait<void>([
        for (final registration in registrations)
          registration.close(remove: false),
        if (delegate != null) delegate.close(timeout: effectiveTimeout),
      ], eagerError: false);
      try {
        await cleanup.timeout(_remainingUntil(deadline));
      } on Object catch (error, stackTrace) {
        captureFailure(error, stackTrace);
      }
    } on Object catch (error, stackTrace) {
      captureFailure(error, stackTrace);
    } finally {
      _closing = false;
    }
    if (failure != null) {
      Error.throwWithStackTrace(failure!, failureStackTrace!);
    }
  }
''';

const _durableQueueCloseBefore = '''  @override
  Future<void> close({Duration? timeout}) async {
    if (_closing) {
      return;
    }
    _closing = true;
    _generation += 1;
    _stopHealthMonitor();
    final effectiveTimeout = timeout ?? policy.recoveryTimeout;
    Object? failure;
    StackTrace? failureStackTrace;
    try {
      final connecting = _connecting;
      if (connecting != null) {
        try {
          await connecting.timeout(effectiveTimeout);
        } on Object {
          // A concurrent connect is cancelled or failed; cleanup continues.
        }
      }
      final recovery = _recovery;
      if (recovery != null) {
        try {
          await recovery.timeout(effectiveTimeout);
        } on Object {
          // Shutdown cancels recovery; cleanup continues with the delegate.
        }
      }
      await Future.wait([
        for (final registration in _registrations.toList())
          registration.close(remove: false),
      ]).timeout(effectiveTimeout);
      _registrations.clear();
      final delegate = _delegate;
      _delegate = null;
      if (delegate != null) {
        await delegate
            .close(timeout: effectiveTimeout)
            .timeout(effectiveTimeout);
      }
    } on Object catch (error, stackTrace) {
      failure = error;
      failureStackTrace = stackTrace;
    } finally {
      _closing = false;
    }
    if (failure != null) {
      Error.throwWithStackTrace(failure, failureStackTrace!);
    }
  }
''';

const _durableQueueCloseAfter = '''  @override
  Future<void> close({Duration? timeout}) async {
    if (_closing) {
      return;
    }
    _closing = true;
    _generation += 1;
    _stopHealthMonitor();
    final effectiveTimeout = timeout ?? policy.recoveryTimeout;
    final deadline = DateTime.now().add(effectiveTimeout);
    Object? failure;
    StackTrace? failureStackTrace;

    void captureFailure(Object error, StackTrace stackTrace) {
      failure ??= error;
      failureStackTrace ??= stackTrace;
    }

    try {
      await Future.wait([
        if (_connecting case final connecting?)
          _ignoreUntilDeadline(connecting, deadline),
        if (_recovery case final recovery?)
          _ignoreUntilDeadline(recovery, deadline),
      ]);

      final registrations = _registrations.toList();
      _registrations.clear();
      final delegate = _delegate;
      _delegate = null;
      final cleanup = Future.wait<void>([
        for (final registration in registrations)
          registration.close(remove: false),
        if (delegate != null) delegate.close(timeout: effectiveTimeout),
      ], eagerError: false);
      try {
        await cleanup.timeout(_remainingUntil(deadline));
      } on Object catch (error, stackTrace) {
        captureFailure(error, stackTrace);
      }
    } on Object catch (error, stackTrace) {
      captureFailure(error, stackTrace);
    } finally {
      _closing = false;
    }
    if (failure != null) {
      Error.throwWithStackTrace(failure!, failureStackTrace!);
    }
  }
''';

const _regressionTest = r'''import 'dart:async';

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
  Future<void> publish<T>(String subject, T payload, {MessageHeaders? headers}) async {}

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
''';
