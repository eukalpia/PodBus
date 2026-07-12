// ignore_for_file: prefer_initializing_formals

import 'dart:async';
import 'dart:math' as math;

import 'capabilities.dart';
import 'durable_job_queue.dart';
import 'exceptions.dart';
import 'headers.dart';
import 'health.dart';
import 'message_bus.dart';
import 'policies.dart';

typedef MessageBusFactory = FutureOr<MessageBus> Function();
typedef DurableJobQueueFactory = FutureOr<DurableJobQueue> Function();
typedef ReconnectErrorPredicate = bool Function(Object error);

/// Controls process-level recovery when a broker client loses its connection.
///
/// The transport remains responsible for its protocol semantics. This policy
/// only governs how PodBus recreates a client, reconnects it, and re-registers
/// active subscriptions or workers.
final class ReconnectPolicy {
  const ReconnectPolicy({
    this.maxAttempts = 8,
    this.initialDelay = const Duration(milliseconds: 100),
    this.maxDelay = const Duration(seconds: 10),
    this.backoffMultiplier = 2,
    this.jitter = 0.2,
    this.recoveryTimeout = const Duration(seconds: 30),
    this.healthCheckInterval = const Duration(seconds: 5),
    this.healthCheckTimeout = const Duration(seconds: 2),
  }) : assert(maxAttempts > 0),
       assert(backoffMultiplier >= 1),
       assert(jitter >= 0 && jitter <= 1);

  final int maxAttempts;
  final Duration initialDelay;
  final Duration maxDelay;
  final double backoffMultiplier;
  final double jitter;
  final Duration recoveryTimeout;

  /// Interval for proactive broker health probes. Set to `null` to disable
  /// background recovery and only reconnect after a failed operation.
  final Duration? healthCheckInterval;

  /// Maximum time allowed for a proactive broker health probe.
  final Duration healthCheckTimeout;

  Duration delayForAttempt(int attempt, {math.Random? random}) {
    if (attempt <= 0 || initialDelay == Duration.zero) {
      return Duration.zero;
    }
    final rawMicros =
        initialDelay.inMicroseconds *
        math.pow(backoffMultiplier, attempt - 1).toDouble();
    final cappedMicros = math.min(
      rawMicros,
      maxDelay.inMicroseconds.toDouble(),
    );
    if (jitter == 0) {
      return Duration(microseconds: cappedMicros.round());
    }
    final source = random ?? math.Random();
    final factor = 1 - jitter + source.nextDouble() * jitter * 2;
    return Duration(microseconds: math.max(0, (cappedMicros * factor).round()));
  }
}

bool defaultReconnectErrorPredicate(Object error) {
  return error is MessagingConnectionException ||
      error is MessagingTimeoutException;
}

void _validateReconnectPolicy(ReconnectPolicy policy) {
  if (policy.maxAttempts < 1) {
    throw const MessagingConfigurationException(
      'Reconnect maxAttempts must be greater than zero.',
    );
  }
  if (policy.initialDelay.isNegative || policy.maxDelay.isNegative) {
    throw const MessagingConfigurationException(
      'Reconnect delays must not be negative.',
    );
  }
  if (policy.maxDelay < policy.initialDelay) {
    throw const MessagingConfigurationException(
      'Reconnect maxDelay must be greater than or equal to initialDelay.',
    );
  }
  if (policy.backoffMultiplier < 1) {
    throw const MessagingConfigurationException(
      'Reconnect backoffMultiplier must be at least one.',
    );
  }
  if (policy.jitter < 0 || policy.jitter > 1) {
    throw const MessagingConfigurationException(
      'Reconnect jitter must be between zero and one.',
    );
  }
  if (policy.recoveryTimeout <= Duration.zero ||
      policy.healthCheckTimeout <= Duration.zero) {
    throw const MessagingConfigurationException(
      'Reconnect timeouts must be greater than zero.',
    );
  }
  final healthCheckInterval = policy.healthCheckInterval;
  if (healthCheckInterval != null && healthCheckInterval <= Duration.zero) {
    throw const MessagingConfigurationException(
      'Reconnect healthCheckInterval must be greater than zero.',
    );
  }
}

/// Recreates a [MessageBus] after connection failures and restores active
/// subscriptions before retrying the failed operation once.
///
/// This wrapper is framework-neutral and can be used by plain Dart servers,
/// command-line daemons, isolates, or Serverpod applications.
final class ResilientMessageBus implements MessageBus {
  ResilientMessageBus({
    required MessageBusFactory factory,
    this.policy = const ReconnectPolicy(),
    ReconnectErrorPredicate? shouldReconnect,
    FutureOr<void> Function(Object error, int attempt, Duration delay)?
    onReconnectAttempt,
  }) : _factory = factory,
       _shouldReconnect = shouldReconnect ?? defaultReconnectErrorPredicate,
       _onReconnectAttempt = onReconnectAttempt {
    _validateReconnectPolicy(policy);
  }

  final MessageBusFactory _factory;
  final ReconnectPolicy policy;
  final ReconnectErrorPredicate _shouldReconnect;
  final FutureOr<void> Function(Object error, int attempt, Duration delay)?
  _onReconnectAttempt;
  final List<_SubscriptionRegistration<Object?>> _registrations = [];

  MessageBus? _delegate;
  Future<void>? _connecting;
  Future<void>? _recovery;
  Timer? _healthTimer;
  var _probeActive = false;
  var _closing = false;
  var _generation = 0;
  Object? _lastRecoveryError;
  DateTime? _lastRecoveredAt;

  @override
  MessagingCapabilities get capabilities =>
      _delegate?.capabilities ?? MessagingCapabilities.none;

  @override
  Future<void> connect() async {
    if (_delegate != null) {
      return;
    }
    final existing = _connecting;
    if (existing != null) {
      return existing;
    }
    _closing = false;
    final generation = _generation;
    late final Future<void> connecting;
    connecting = () async {
      final candidate = await _createConnectedDelegate();
      if (_closing || generation != _generation) {
        await candidate.close(timeout: policy.recoveryTimeout);
        throw const MessagingConnectionException(
          'Message bus connection was cancelled by shutdown.',
        );
      }
      _delegate = candidate;
      _startHealthMonitor();
    }();
    _connecting = connecting;
    try {
      await connecting;
    } finally {
      if (identical(_connecting, connecting)) {
        _connecting = null;
      }
    }
  }

  @override
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

  @override
  Future<void> publish<T>(
    String subject,
    T payload, {
    MessageHeaders? headers,
  }) {
    return _execute(
      (delegate) => delegate.publish(subject, payload, headers: headers),
    );
  }

  @override
  Future<TResponse> request<TRequest, TResponse>(
    String subject,
    TRequest payload, {
    MessageHeaders? headers,
    Duration? timeout,
  }) {
    return _execute(
      (delegate) => delegate.request<TRequest, TResponse>(
        subject,
        payload,
        headers: headers,
        timeout: timeout,
      ),
    );
  }

  @override
  Future<Subscription> subscribe<T>(
    String subject, {
    String? queueGroup,
    int concurrency = 1,
    required MessageHandler<T> handler,
  }) async {
    if (concurrency < 1) {
      throw const MessagingConfigurationException(
        'Subscription concurrency must be greater than zero.',
      );
    }
    final registration = _SubscriptionRegistration<T>(
      owner: this,
      subject: subject,
      queueGroup: queueGroup,
      concurrency: concurrency,
      handler: handler,
    );
    await _execute((delegate) => registration.bind(delegate));
    _registrations.add(registration as _SubscriptionRegistration<Object?>);
    return registration;
  }

  @override
  Future<HealthCheckResult> healthCheck() async {
    final recovery = _recovery;
    if (recovery != null) {
      return HealthCheckResult.degraded(
        message: 'Message bus is recovering its broker connection.',
        details: {
          'subscriptions': _registrations.length,
          if (_lastRecoveryError != null)
            'lastRecoveryError': _lastRecoveryError.toString(),
        },
      );
    }
    final delegate = _delegate;
    if (delegate == null) {
      return HealthCheckResult.unhealthy(
        message: 'Message bus has not been connected.',
      );
    }
    final result = await delegate.healthCheck();
    return HealthCheckResult(
      status: result.status,
      checkedAt: result.checkedAt,
      message: result.message,
      details: {
        ...result.details,
        'resilientSubscriptions': _registrations.length,
        if (_lastRecoveredAt != null)
          'lastRecoveredAt': _lastRecoveredAt!.toIso8601String(),
        if (_lastRecoveryError != null)
          'lastRecoveryError': _lastRecoveryError.toString(),
      },
    );
  }

  Future<R> _execute<R>(
    Future<R> Function(MessageBus delegate) operation,
  ) async {
    if (_closing) {
      throw const MessagingConnectionException('Message bus is shutting down.');
    }
    await connect();
    final recovery = _recovery;
    if (recovery != null) {
      await recovery;
    }
    try {
      return await operation(_requireDelegate());
    } on Object catch (error, stackTrace) {
      if (!_shouldReconnect(error) || _closing) {
        Error.throwWithStackTrace(error, stackTrace);
      }
      await _recover(error);
      return operation(_requireDelegate());
    }
  }

  Future<void> _recover(Object trigger) {
    final existing = _recovery;
    if (existing != null) {
      return existing;
    }
    final completer = Completer<void>();
    final generation = _generation;
    _recovery = completer.future;
    unawaited(() async {
      Object? lastError = trigger;
      StackTrace? lastStackTrace;
      try {
        for (var attempt = 1; attempt <= policy.maxAttempts; attempt += 1) {
          final delay = policy.delayForAttempt(attempt);
          await _onReconnectAttempt?.call(lastError!, attempt, delay);
          if (delay > Duration.zero) {
            await Future<void>.delayed(delay);
          }
          if (_closing || generation != _generation) {
            throw const MessagingConnectionException(
              'Message bus recovery was cancelled by shutdown.',
            );
          }
          MessageBus? replacement;
          try {
            await _disposeDelegate();
            replacement = await _createConnectedDelegate();
            if (_closing || generation != _generation) {
              throw const MessagingConnectionException(
                'Message bus recovery was cancelled by shutdown.',
              );
            }
            for (final registration in _registrations.toList()) {
              if (!registration.isClosed) {
                await registration.bind(replacement);
              }
            }
            _delegate = replacement;
            replacement = null;
            _lastRecoveryError = null;
            _lastRecoveredAt = DateTime.now().toUtc();
            completer.complete();
            return;
          } on Object catch (error, stackTrace) {
            if (replacement != null) {
              try {
                await replacement.close(timeout: policy.recoveryTimeout);
              } on Object {
                // Preserve the recovery failure.
              }
            }
            lastError = error;
            lastStackTrace = stackTrace;
            _lastRecoveryError = error;
          }
        }
        throw MessagingConnectionException(
          'Message bus recovery exhausted ${policy.maxAttempts} attempts.',
          cause: lastError,
          stackTrace: lastStackTrace,
        );
      } on Object catch (error, stackTrace) {
        _lastRecoveryError = error;
        completer.completeError(error, stackTrace);
      } finally {
        if (identical(_recovery, completer.future)) {
          _recovery = null;
        }
      }
    }());
    return completer.future.timeout(policy.recoveryTimeout);
  }

  Future<MessageBus> _createConnectedDelegate() async {
    final delegate = await _factory();
    try {
      await delegate.connect();
      return delegate;
    } on Object catch (error, stackTrace) {
      try {
        await delegate.close(timeout: policy.recoveryTimeout);
      } on Object {
        // Preserve the connection failure.
      }
      Error.throwWithStackTrace(error, stackTrace);
    }
  }

  void _startHealthMonitor() {
    _stopHealthMonitor();
    final interval = policy.healthCheckInterval;
    if (interval == null || _closing) {
      return;
    }
    _healthTimer = Timer.periodic(interval, (_) {
      unawaited(_probeHealth());
    });
  }

  void _stopHealthMonitor() {
    _healthTimer?.cancel();
    _healthTimer = null;
  }

  Future<void> _probeHealth() async {
    if (_probeActive || _closing || _recovery != null) {
      return;
    }
    final delegate = _delegate;
    if (delegate == null) {
      return;
    }
    _probeActive = true;
    try {
      final result = await delegate.healthCheck().timeout(
        policy.healthCheckTimeout,
      );
      if (result.status == HealthStatus.unhealthy && !_closing) {
        await _recover(
          MessagingConnectionException(
            'Message bus health probe reported an unhealthy transport: '
            '${result.message ?? 'no details'}',
          ),
        );
      }
    } on Object catch (error) {
      if (!_closing) {
        try {
          await _recover(
            MessagingConnectionException(
              'Message bus health probe failed.',
              cause: error,
            ),
          );
        } on Object {
          // The next probe or foreground operation will retry recovery.
        }
      }
    } finally {
      _probeActive = false;
    }
  }

  Future<void> _disposeDelegate() async {
    final delegate = _delegate;
    _delegate = null;
    if (delegate == null) {
      return;
    }
    try {
      await delegate.close(timeout: policy.recoveryTimeout);
    } on Object {
      // Recovery continues with a fresh transport instance.
    }
  }

  MessageBus _requireDelegate() {
    final delegate = _delegate;
    if (delegate == null) {
      throw const MessagingConnectionException('Message bus is not connected.');
    }
    return delegate;
  }

  void _remove(_SubscriptionRegistration<Object?> registration) {
    _registrations.remove(registration);
  }
}

/// Recreates a [DurableJobQueue] after connection failures and restores active
/// worker registrations before retrying enqueue operations once.
final class ResilientDurableJobQueue implements DurableJobQueue {
  ResilientDurableJobQueue({
    required DurableJobQueueFactory factory,
    this.policy = const ReconnectPolicy(),
    ReconnectErrorPredicate? shouldReconnect,
    FutureOr<void> Function(Object error, int attempt, Duration delay)?
    onReconnectAttempt,
  }) : _factory = factory,
       _shouldReconnect = shouldReconnect ?? defaultReconnectErrorPredicate,
       _onReconnectAttempt = onReconnectAttempt {
    _validateReconnectPolicy(policy);
  }

  final DurableJobQueueFactory _factory;
  final ReconnectPolicy policy;
  final ReconnectErrorPredicate _shouldReconnect;
  final FutureOr<void> Function(Object error, int attempt, Duration delay)?
  _onReconnectAttempt;
  final List<_WorkerRegistration<Object?>> _registrations = [];

  DurableJobQueue? _delegate;
  Future<void>? _connecting;
  Future<void>? _recovery;
  Timer? _healthTimer;
  var _probeActive = false;
  var _closing = false;
  var _generation = 0;
  Object? _lastRecoveryError;
  DateTime? _lastRecoveredAt;

  @override
  MessagingCapabilities get capabilities =>
      _delegate?.capabilities ?? MessagingCapabilities.none;

  @override
  Future<void> connect() async {
    if (_delegate != null) {
      return;
    }
    final existing = _connecting;
    if (existing != null) {
      return existing;
    }
    _closing = false;
    final generation = _generation;
    late final Future<void> connecting;
    connecting = () async {
      final candidate = await _createConnectedDelegate();
      if (_closing || generation != _generation) {
        await candidate.close(timeout: policy.recoveryTimeout);
        throw const MessagingConnectionException(
          'Durable queue connection was cancelled by shutdown.',
        );
      }
      _delegate = candidate;
      _startHealthMonitor();
    }();
    _connecting = connecting;
    try {
      await connecting;
    } finally {
      if (identical(_connecting, connecting)) {
        _connecting = null;
      }
    }
  }

  @override
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

  @override
  Future<void> enqueue<T>(
    String topic,
    T payload, {
    MessageHeaders? headers,
    String? idempotencyKey,
    DateTime? runAt,
    RetryPolicy? retryPolicy,
  }) {
    return _execute(
      (delegate) => delegate.enqueue(
        topic,
        payload,
        headers: headers,
        idempotencyKey: idempotencyKey,
        runAt: runAt,
        retryPolicy: retryPolicy,
      ),
    );
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
    if (concurrency < 1) {
      throw const MessagingConfigurationException(
        'Worker concurrency must be greater than zero.',
      );
    }
    final registration = _WorkerRegistration<T>(
      owner: this,
      topic: topic,
      queueGroup: queueGroup,
      durableName: durableName,
      concurrency: concurrency,
      retryPolicy: retryPolicy,
      deadLetterPolicy: deadLetterPolicy,
      handler: handler,
    );
    await _execute((delegate) => registration.bind(delegate));
    _registrations.add(registration as _WorkerRegistration<Object?>);
    return registration;
  }

  @override
  Future<HealthCheckResult> healthCheck() async {
    final recovery = _recovery;
    if (recovery != null) {
      return HealthCheckResult.degraded(
        message: 'Durable job queue is recovering its broker connection.',
        details: {
          'workers': _registrations.length,
          if (_lastRecoveryError != null)
            'lastRecoveryError': _lastRecoveryError.toString(),
        },
      );
    }
    final delegate = _delegate;
    if (delegate == null) {
      return HealthCheckResult.unhealthy(
        message: 'Durable job queue has not been connected.',
      );
    }
    final result = await delegate.healthCheck();
    return HealthCheckResult(
      status: result.status,
      checkedAt: result.checkedAt,
      message: result.message,
      details: {
        ...result.details,
        'resilientWorkers': _registrations.length,
        if (_lastRecoveredAt != null)
          'lastRecoveredAt': _lastRecoveredAt!.toIso8601String(),
        if (_lastRecoveryError != null)
          'lastRecoveryError': _lastRecoveryError.toString(),
      },
    );
  }

  Future<R> _execute<R>(
    Future<R> Function(DurableJobQueue delegate) operation,
  ) async {
    if (_closing) {
      throw const MessagingConnectionException(
        'Durable job queue is shutting down.',
      );
    }
    await connect();
    final recovery = _recovery;
    if (recovery != null) {
      await recovery;
    }
    try {
      return await operation(_requireDelegate());
    } on Object catch (error, stackTrace) {
      if (!_shouldReconnect(error) || _closing) {
        Error.throwWithStackTrace(error, stackTrace);
      }
      await _recover(error);
      return operation(_requireDelegate());
    }
  }

  Future<void> _recover(Object trigger) {
    final existing = _recovery;
    if (existing != null) {
      return existing;
    }
    final completer = Completer<void>();
    final generation = _generation;
    _recovery = completer.future;
    unawaited(() async {
      Object? lastError = trigger;
      StackTrace? lastStackTrace;
      try {
        for (var attempt = 1; attempt <= policy.maxAttempts; attempt += 1) {
          final delay = policy.delayForAttempt(attempt);
          await _onReconnectAttempt?.call(lastError!, attempt, delay);
          if (delay > Duration.zero) {
            await Future<void>.delayed(delay);
          }
          if (_closing || generation != _generation) {
            throw const MessagingConnectionException(
              'Durable queue recovery was cancelled by shutdown.',
            );
          }
          DurableJobQueue? replacement;
          try {
            await _disposeDelegate();
            replacement = await _createConnectedDelegate();
            if (_closing || generation != _generation) {
              throw const MessagingConnectionException(
                'Durable queue recovery was cancelled by shutdown.',
              );
            }
            for (final registration in _registrations.toList()) {
              if (!registration.isClosed) {
                await registration.bind(replacement);
              }
            }
            _delegate = replacement;
            replacement = null;
            _lastRecoveryError = null;
            _lastRecoveredAt = DateTime.now().toUtc();
            completer.complete();
            return;
          } on Object catch (error, stackTrace) {
            if (replacement != null) {
              try {
                await replacement.close(timeout: policy.recoveryTimeout);
              } on Object {
                // Preserve the recovery failure.
              }
            }
            lastError = error;
            lastStackTrace = stackTrace;
            _lastRecoveryError = error;
          }
        }
        throw MessagingConnectionException(
          'Durable queue recovery exhausted ${policy.maxAttempts} attempts.',
          cause: lastError,
          stackTrace: lastStackTrace,
        );
      } on Object catch (error, stackTrace) {
        _lastRecoveryError = error;
        completer.completeError(error, stackTrace);
      } finally {
        if (identical(_recovery, completer.future)) {
          _recovery = null;
        }
      }
    }());
    return completer.future.timeout(policy.recoveryTimeout);
  }

  Future<DurableJobQueue> _createConnectedDelegate() async {
    final delegate = await _factory();
    try {
      await delegate.connect();
      return delegate;
    } on Object catch (error, stackTrace) {
      try {
        await delegate.close(timeout: policy.recoveryTimeout);
      } on Object {
        // Preserve the connection failure.
      }
      Error.throwWithStackTrace(error, stackTrace);
    }
  }

  void _startHealthMonitor() {
    _stopHealthMonitor();
    final interval = policy.healthCheckInterval;
    if (interval == null || _closing) {
      return;
    }
    _healthTimer = Timer.periodic(interval, (_) {
      unawaited(_probeHealth());
    });
  }

  void _stopHealthMonitor() {
    _healthTimer?.cancel();
    _healthTimer = null;
  }

  Future<void> _probeHealth() async {
    if (_probeActive || _closing || _recovery != null) {
      return;
    }
    final delegate = _delegate;
    if (delegate == null) {
      return;
    }
    _probeActive = true;
    try {
      final result = await delegate.healthCheck().timeout(
        policy.healthCheckTimeout,
      );
      if (result.status == HealthStatus.unhealthy && !_closing) {
        await _recover(
          MessagingConnectionException(
            'Durable queue health probe reported an unhealthy transport: '
            '${result.message ?? 'no details'}',
          ),
        );
      }
    } on Object catch (error) {
      if (!_closing) {
        try {
          await _recover(
            MessagingConnectionException(
              'Durable queue health probe failed.',
              cause: error,
            ),
          );
        } on Object {
          // The next probe or foreground operation will retry recovery.
        }
      }
    } finally {
      _probeActive = false;
    }
  }

  Future<void> _disposeDelegate() async {
    final delegate = _delegate;
    _delegate = null;
    if (delegate == null) {
      return;
    }
    try {
      await delegate.close(timeout: policy.recoveryTimeout);
    } on Object {
      // Recovery continues with a fresh transport instance.
    }
  }

  DurableJobQueue _requireDelegate() {
    final delegate = _delegate;
    if (delegate == null) {
      throw const MessagingConnectionException(
        'Durable job queue is not connected.',
      );
    }
    return delegate;
  }

  void _remove(_WorkerRegistration<Object?> registration) {
    _registrations.remove(registration);
  }
}

final class _SubscriptionRegistration<T> implements Subscription {
  _SubscriptionRegistration({
    required this.owner,
    required this.subject,
    required this.queueGroup,
    required this.concurrency,
    required this.handler,
  });

  final ResilientMessageBus owner;
  final String subject;
  final String? queueGroup;
  final int concurrency;
  final MessageHandler<T> handler;
  Subscription? _delegate;
  var isClosed = false;

  Future<void> bind(MessageBus bus) async {
    final previous = _delegate;
    final replacement = await bus.subscribe<T>(
      subject,
      queueGroup: queueGroup,
      concurrency: concurrency,
      handler: handler,
    );
    _delegate = replacement;
    if (previous != null) {
      try {
        await previous.close();
      } on Object {
        // The previous transport may already be disconnected.
      }
    }
  }

  @override
  Future<void> close({bool remove = true}) async {
    if (isClosed) {
      return;
    }
    isClosed = true;
    final delegate = _delegate;
    _delegate = null;
    if (delegate != null) {
      await delegate.close();
    }
    if (remove) {
      owner._remove(this as _SubscriptionRegistration<Object?>);
    }
  }
}

final class _WorkerRegistration<T> implements Worker {
  _WorkerRegistration({
    required this.owner,
    required this.topic,
    required this.queueGroup,
    required this.durableName,
    required this.concurrency,
    required this.retryPolicy,
    required this.deadLetterPolicy,
    required this.handler,
  });

  final ResilientDurableJobQueue owner;
  final String topic;
  final String? queueGroup;
  final String? durableName;
  final int concurrency;
  final RetryPolicy? retryPolicy;
  final DeadLetterPolicy? deadLetterPolicy;
  final JobHandler<T> handler;
  Worker? _delegate;
  var isClosed = false;

  Future<void> bind(DurableJobQueue queue) async {
    final previous = _delegate;
    final replacement = await queue.worker<T>(
      topic,
      queueGroup: queueGroup,
      durableName: durableName,
      concurrency: concurrency,
      retryPolicy: retryPolicy,
      deadLetterPolicy: deadLetterPolicy,
      handler: handler,
    );
    _delegate = replacement;
    if (previous != null) {
      try {
        await previous.close();
      } on Object {
        // The previous transport may already be disconnected.
      }
    }
  }

  @override
  Future<void> close({bool remove = true}) async {
    if (isClosed) {
      return;
    }
    isClosed = true;
    final delegate = _delegate;
    _delegate = null;
    if (delegate != null) {
      await delegate.close();
    }
    if (remove) {
      owner._remove(this as _WorkerRegistration<Object?>);
    }
  }
}
