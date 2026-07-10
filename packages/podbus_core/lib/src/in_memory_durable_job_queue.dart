import 'dart:async';
import 'dart:collection';

import 'package:uuid/uuid.dart';

import 'capabilities.dart';
import 'config.dart';
import 'durable_job_queue.dart';
import 'exceptions.dart';
import 'headers.dart';
import 'health.dart';
import 'idempotency_store.dart';
import 'message_context.dart';
import 'policies.dart';
import 'wire_protocol.dart';

final class InMemoryDurableJobQueue implements DurableJobQueue {
  InMemoryDurableJobQueue({
    MessagingConfig? messagingConfig,
    IdempotencyStore? idempotencyStore,
    Duration idempotencyTtl = const Duration(hours: 24),
    Uuid? uuid,
  }) : messagingConfig = messagingConfig ?? MessagingConfig(),
       _idempotencyStore =
           idempotencyStore ?? messagingConfig?.idempotencyStore,
       _idempotencyTtl = idempotencyTtl,
       _uuid = uuid ?? const Uuid();

  static const _capabilities = MessagingCapabilities({
    MessagingCapability.durableJobs,
    MessagingCapability.delayedDelivery,
    MessagingCapability.retries,
    MessagingCapability.deadLettering,
    MessagingCapability.idempotentPublish,
    MessagingCapability.manualAcknowledgement,
    MessagingCapability.gracefulShutdown,
  });

  final MessagingConfig messagingConfig;
  final IdempotencyStore? _idempotencyStore;
  final Duration _idempotencyTtl;
  final Uuid _uuid;
  final Map<String, Queue<_QueuedJob>> _jobsByTopic = {};
  final List<_WorkerBinding> _workers = [];
  final Map<String, int> _workerOffsets = {};
  final Set<Future<void>> _activeTasks = {};
  final Set<Timer> _timers = {};
  var _connected = false;
  var _closing = false;

  @override
  MessagingCapabilities get capabilities => _capabilities;

  @override
  Future<void> connect() async {
    _closing = false;
    _connected = true;
    await _drainAll();
  }

  @override
  Future<void> close({Duration? timeout}) async {
    if (!_connected && !_closing) {
      return;
    }
    _closing = true;
    _connected = false;
    for (final timer in _timers.toList()) {
      timer.cancel();
    }
    _timers.clear();

    for (final worker in _workers.toList()) {
      await worker.close();
    }

    final effectiveTimeout = timeout ?? messagingConfig.shutdownTimeout;
    if (_activeTasks.isNotEmpty) {
      await Future.wait(_activeTasks.toList()).timeout(
        effectiveTimeout,
        onTimeout: () {
          throw MessagingTimeoutException(
            'In-memory job queue did not drain within $effectiveTimeout.',
          );
        },
      );
    }

    _jobsByTopic.clear();
    _workers.clear();
    _workerOffsets.clear();
    _closing = false;
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
    _ensureConnected();

    final effectiveHeaders = (headers ?? MessageHeaders()).copyWith(
      idempotencyKey: idempotencyKey,
    );
    messagingConfig.limits.validateHeaders(effectiveHeaders.toMap());
    final key = idempotencyKey ?? effectiveHeaders.idempotencyKey;
    var claimed = false;
    if (key != null && _idempotencyStore != null) {
      claimed = await _idempotencyStore.claim(key, ttl: _idempotencyTtl);
      if (!claimed) {
        messagingConfig.recordMetric(
          'podbus.jobs.deduplicated',
          attributes: {'transport': 'memory', 'topic': topic},
        );
        return;
      }
    }

    try {
      final job = _QueuedJob(
        id: _uuid.v4(),
        topic: topic,
        payload: payload,
        headers: effectiveHeaders,
        runAt: runAt,
        enqueueRetryPolicy: retryPolicy,
      );
      _jobsByTopic.putIfAbsent(topic, Queue.new).add(job);
      await _drainTopic(topic);
      messagingConfig.recordMetric(
        'podbus.jobs.enqueued',
        attributes: {'transport': 'memory', 'topic': topic},
      );
    } on Object {
      if (claimed && key != null && _idempotencyStore != null) {
        await _idempotencyStore.release(key);
      }
      rethrow;
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
    required Future<void> Function(JobContext context, T payload) handler,
  }) async {
    _ensureConnected();
    if (concurrency < 1) {
      throw const MessagingConfigurationException(
        'Worker concurrency must be greater than zero.',
      );
    }

    final worker = _InMemoryWorker<T>(
      topic: topic,
      queueGroup: queueGroup,
      durableName: durableName,
      concurrency: concurrency,
      retryPolicy: retryPolicy,
      deadLetterPolicy: deadLetterPolicy ?? const DeadLetterPolicy.disabled(),
      handler: handler,
      onClose: (_WorkerBinding worker) {
        _workers.remove(worker);
      },
    );
    _workers.add(worker);
    await _drainTopic(topic);
    return worker;
  }

  @override
  Future<HealthCheckResult> healthCheck() async {
    if (!_connected) {
      return HealthCheckResult.unhealthy(
        message: _closing
            ? 'In-memory durable queue is draining.'
            : 'In-memory durable queue is not connected.',
        details: {'activeTasks': _activeTasks.length},
      );
    }
    return HealthCheckResult.healthy(
      message: 'In-memory durable queue is connected.',
      details: {
        'workers': _workers.length,
        'topics': _jobsByTopic.length,
        'activeTasks': _activeTasks.length,
        'scheduledTasks': _timers.length,
      },
    );
  }

  Future<void> _drainAll() async {
    for (final topic in _jobsByTopic.keys.toList()) {
      await _drainTopic(topic);
    }
  }

  Future<void> _drainTopic(String topic) async {
    if (!_connected || _closing) {
      return;
    }

    final queue = _jobsByTopic[topic];
    if (queue == null || queue.isEmpty) {
      return;
    }

    while (queue.isNotEmpty) {
      final worker = _nextAvailableWorker(topic);
      if (worker == null) {
        return;
      }

      final job = queue.removeFirst();
      final runAt = job.runAt;
      final now = messagingConfig.now();
      if (runAt != null && runAt.isAfter(now)) {
        queue.addFirst(job);
        _schedule(runAt.difference(now), () => _drainTopic(topic));
        return;
      }

      _startTask(_process(worker, job));
    }
  }

  _WorkerBinding? _nextAvailableWorker(String topic) {
    final candidates = [
      for (final worker in _workers)
        if (!worker.closed &&
            worker.topic == topic &&
            worker.active < worker.concurrency)
          worker,
    ];
    if (candidates.isEmpty) {
      return null;
    }

    final offset = _workerOffsets[topic] ?? 0;
    _workerOffsets[topic] = offset + 1;
    return candidates[offset % candidates.length];
  }

  Future<void> _process(_WorkerBinding worker, _QueuedJob job) async {
    worker.active++;
    final startedAt = messagingConfig.now();
    final attempt = job.attempt + 1;
    final retryPolicy =
        worker.retryPolicy ??
        job.enqueueRetryPolicy ??
        messagingConfig.defaultRetryPolicy;

    final headerAttempt = job.headers.attempt > attempt
        ? job.headers.attempt
        : attempt;
    final context = _InMemoryJobContext(
      topic: job.topic,
      headers: job.headers.copyWith(attempt: headerAttempt),
      rawMessage: job.payload,
      attempt: attempt,
      maxAttempts: retryPolicy.maxAttempts,
      queue: this,
      worker: worker,
      job: job,
    );

    try {
      await worker.deliver(context, job.payload);
      if (!context.completed) {
        await context.ack();
      }
      messagingConfig.recordMetric(
        'podbus.jobs.completed',
        attributes: {'transport': 'memory', 'topic': job.topic},
      );
    } on Object catch (error, stackTrace) {
      await _handleFailure(
        worker,
        job,
        attempt,
        retryPolicy,
        error,
        stackTrace,
      );
    } finally {
      worker.active--;
      messagingConfig.recordDuration(
        'podbus.job.duration',
        messagingConfig.now().difference(startedAt),
        attributes: {'transport': 'memory', 'topic': job.topic},
      );
      await _drainTopic(job.topic);
    }
  }

  Future<void> _handleFailure(
    _WorkerBinding worker,
    _QueuedJob job,
    int attempt,
    RetryPolicy retryPolicy,
    Object error,
    StackTrace stackTrace,
  ) async {
    final retryable = messagingConfig.shouldRetry(error);
    if (retryable && attempt < retryPolicy.maxAttempts) {
      final next = job.copyForAttempt(attempt);
      final delay = retryPolicy.delayForAttempt(attempt);
      messagingConfig.recordMetric(
        'podbus.jobs.retried',
        attributes: {
          'transport': 'memory',
          'topic': job.topic,
          'attempt': attempt,
        },
      );
      _schedule(delay, () async {
        if (!_connected || _closing) {
          return;
        }
        _jobsByTopic.putIfAbsent(job.topic, Queue.new).add(next);
        await _drainTopic(job.topic);
      });
      return;
    }

    final policy = worker.deadLetterPolicy;
    if (policy.enabled) {
      await _publishDeadLetter(
        worker,
        job,
        attempt,
        error: error,
        stackTrace: stackTrace,
      );
      return;
    }

    messagingConfig.log(
      MessagingLogLevel.error,
      'Job failed without a dead-letter destination.',
      error: error,
      stackTrace: stackTrace,
      attributes: {
        'transport': 'memory',
        'topic': job.topic,
        'attempt': attempt,
      },
    );
  }

  Future<void> _publishDeadLetter(
    _WorkerBinding worker,
    _QueuedJob job,
    int attempt, {
    Object? error,
    StackTrace? stackTrace,
  }) async {
    final policy = worker.deadLetterPolicy;
    if (!policy.enabled) {
      return;
    }

    final destination = policy.destination ?? '${worker.topic}.dead-letter';
    final custom = <String, String>{
      ...job.headers.custom,
      PodBusWireHeaders.deadLetterSource: worker.topic,
      if (!policy.includeOriginalPayload)
        PodBusWireHeaders.deadLetterPayloadOmitted: 'true',
      if (policy.includeErrorDetails && error != null)
        PodBusWireHeaders.deadLetterError:
            messagingConfig.limits.truncateError(error),
      if (policy.includeErrorDetails && stackTrace != null)
        PodBusWireHeaders.deadLetterStackTrace:
            messagingConfig.limits.truncateError(stackTrace),
    };
    final deadLetterHeaders = job.headers
        .withoutIdempotencyKey()
        .copyWith(attempt: attempt, custom: custom);
    final payload = policy.includeOriginalPayload
        ? job.payload
        : <String, Object?>{
            'source': job.topic,
            'messageId': job.id,
            'payloadIncluded': false,
          };

    await enqueue(
      destination,
      payload,
      headers: deadLetterHeaders,
      retryPolicy: RetryPolicy(
        maxAttempts: 1,
        initialDelay: Duration.zero,
        maxDelay: Duration.zero,
      ),
    );
    messagingConfig.recordMetric(
      'podbus.jobs.dead_lettered',
      attributes: {'transport': 'memory', 'topic': job.topic},
    );
  }

  void _startTask(Future<void> future) {
    _activeTasks.add(future);
    unawaited(
      future.then<void>(
        (_) => _activeTasks.remove(future),
        onError: (Object error, StackTrace stackTrace) {
          _activeTasks.remove(future);
          messagingConfig.log(
            MessagingLogLevel.error,
            'Background job processing failed.',
            error: error,
            stackTrace: stackTrace,
            attributes: {'transport': 'memory'},
          );
        },
      ),
    );
  }

  void _schedule(Duration delay, Future<void> Function() action) {
    late final Timer timer;
    timer = Timer(delay, () {
      _timers.remove(timer);
      _startTask(Future<void>.sync(action));
    });
    _timers.add(timer);
  }

  void _ensureConnected() {
    if (!_connected || _closing) {
      throw const MessagingConnectionException(
        'In-memory durable queue is not connected.',
      );
    }
  }
}

final class _QueuedJob {
  const _QueuedJob({
    required this.id,
    required this.topic,
    required this.payload,
    required this.headers,
    required this.runAt,
    required this.enqueueRetryPolicy,
    this.attempt = 0,
  });

  final String id;
  final String topic;
  final Object? payload;
  final MessageHeaders headers;
  final DateTime? runAt;
  final RetryPolicy? enqueueRetryPolicy;
  final int attempt;

  _QueuedJob copyForAttempt(int attempt) {
    return _QueuedJob(
      id: id,
      topic: topic,
      payload: payload,
      headers: headers.copyWith(attempt: attempt + 1),
      runAt: null,
      enqueueRetryPolicy: enqueueRetryPolicy,
      attempt: attempt,
    );
  }
}

abstract interface class _WorkerBinding implements Worker {
  String get topic;

  String? get queueGroup;

  String? get durableName;

  int get concurrency;

  RetryPolicy? get retryPolicy;

  DeadLetterPolicy get deadLetterPolicy;

  int get active;

  set active(int value);

  bool get closed;

  Future<void> deliver(JobContext context, Object? payload);
}

final class _InMemoryWorker<T> implements Worker, _WorkerBinding {
  _InMemoryWorker({
    required this.topic,
    required this.queueGroup,
    required this.durableName,
    required this.concurrency,
    required this.retryPolicy,
    required this.deadLetterPolicy,
    required this.handler,
    required this.onClose,
  });

  @override
  final String topic;
  @override
  final String? queueGroup;
  @override
  final String? durableName;
  @override
  final int concurrency;
  @override
  final RetryPolicy? retryPolicy;
  @override
  final DeadLetterPolicy deadLetterPolicy;
  final Future<void> Function(JobContext context, T payload) handler;
  final void Function(_WorkerBinding worker) onClose;
  @override
  var active = 0;
  @override
  var closed = false;

  @override
  Future<void> deliver(JobContext context, Object? payload) async {
    if (payload is! T) {
      throw MessageCodecException(
        'Worker for $topic expected $T but received ${payload.runtimeType}.',
      );
    }
    await handler(context, payload);
  }

  @override
  Future<void> close() async {
    if (closed) {
      return;
    }
    closed = true;
    onClose(this);
  }
}

final class _InMemoryJobContext implements JobContext {
  _InMemoryJobContext({
    required this.topic,
    required this.headers,
    required this.rawMessage,
    required this.attempt,
    required this.maxAttempts,
    required this._queue,
    required this._worker,
    required this._job,
  });

  final InMemoryDurableJobQueue _queue;
  final _WorkerBinding _worker;
  final _QueuedJob _job;
  var completed = false;

  @override
  final String topic;

  @override
  final MessageHeaders headers;

  @override
  final Object? rawMessage;

  @override
  final int attempt;

  @override
  final int maxAttempts;

  @override
  Future<void> ack() async {
    completed = true;
  }

  @override
  Future<void> deadLetter({Object? error, StackTrace? stackTrace}) async {
    completed = true;
    await _queue._publishDeadLetter(
      _worker,
      _job,
      attempt,
      error: error,
      stackTrace: stackTrace,
    );
  }

  @override
  Future<void> fail(Object error, [StackTrace? stackTrace]) async {
    Error.throwWithStackTrace(error, stackTrace ?? StackTrace.current);
  }

  @override
  Future<void> retry({Duration? delay}) async {
    completed = true;
    final next = _job.copyForAttempt(attempt);
    _queue._schedule(delay ?? Duration.zero, () async {
      if (!_queue._connected || _queue._closing) {
        return;
      }
      _queue._jobsByTopic.putIfAbsent(topic, Queue.new).add(next);
      await _queue._drainTopic(topic);
    });
  }
}
