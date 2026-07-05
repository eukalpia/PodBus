// ignore_for_file: prefer_initializing_formals

import 'dart:async';
import 'dart:collection';

import 'package:uuid/uuid.dart';

import 'durable_job_queue.dart';
import 'exceptions.dart';
import 'headers.dart';
import 'health.dart';
import 'idempotency_store.dart';
import 'message_context.dart';
import 'policies.dart';

final class InMemoryDurableJobQueue implements DurableJobQueue {
  InMemoryDurableJobQueue({
    IdempotencyStore? idempotencyStore,
    Duration idempotencyTtl = const Duration(hours: 24),
    Uuid? uuid,
  }) : _idempotencyStore = idempotencyStore,
       _idempotencyTtl = idempotencyTtl,
       _uuid = uuid ?? const Uuid();

  final IdempotencyStore? _idempotencyStore;
  final Duration _idempotencyTtl;
  final Uuid _uuid;
  final Map<String, Queue<_QueuedJob>> _jobsByTopic = {};
  final List<_WorkerBinding> _workers = [];
  final Map<String, int> _workerOffsets = {};
  var _connected = false;

  @override
  Future<void> connect() async {
    _connected = true;
    await _drainAll();
  }

  @override
  Future<void> close({Duration? timeout}) async {
    _connected = false;
    _jobsByTopic.clear();
    for (final worker in _workers.toList()) {
      await worker.close();
    }
    _workers.clear();
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
    final key = idempotencyKey ?? effectiveHeaders.idempotencyKey;
    if (key != null && _idempotencyStore != null) {
      final claimed = await _idempotencyStore.claim(key, ttl: _idempotencyTtl);
      if (!claimed) {
        return;
      }
    }

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
        message: 'In-memory durable queue is not connected.',
      );
    }
    return HealthCheckResult.healthy(
      message: 'In-memory durable queue is connected.',
      details: {'workers': _workers.length, 'topics': _jobsByTopic.length},
    );
  }

  Future<void> _drainAll() async {
    for (final topic in _jobsByTopic.keys.toList()) {
      await _drainTopic(topic);
    }
  }

  Future<void> _drainTopic(String topic) async {
    if (!_connected) {
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
      if (runAt != null && runAt.isAfter(DateTime.now())) {
        queue.addFirst(job);
        final delay = runAt.difference(DateTime.now());
        unawaited(Future<void>.delayed(delay, () => _drainTopic(topic)));
        return;
      }

      unawaited(_process(worker, job));
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
    final attempt = job.attempt + 1;
    final retryPolicy =
        worker.retryPolicy ??
        job.enqueueRetryPolicy ??
        RetryPolicy(
          maxAttempts: 1,
          initialDelay: Duration.zero,
          maxDelay: Duration.zero,
        );

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
    if (attempt < retryPolicy.maxAttempts) {
      final next = job.copyForAttempt(attempt);
      final delay = retryPolicy.delayForAttempt(attempt);
      unawaited(
        Future<void>.delayed(delay, () async {
          if (!_connected) {
            return;
          }
          _jobsByTopic.putIfAbsent(job.topic, Queue.new).add(next);
          await _drainTopic(job.topic);
        }),
      );
      return;
    }

    final policy = worker.deadLetterPolicy;
    if (policy.enabled && policy.destination != null) {
      await enqueue(
        policy.destination!,
        job.payload,
        headers: job.headers.copyWith(attempt: attempt),
      );
      return;
    }

    Error.throwWithStackTrace(error, stackTrace);
  }

  void _ensureConnected() {
    if (!_connected) {
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
    final policy = _worker.deadLetterPolicy;
    if (!policy.enabled || policy.destination == null) {
      return;
    }
    await _queue.enqueue(policy.destination!, _job.payload, headers: headers);
  }

  @override
  Future<void> fail(Object error, [StackTrace? stackTrace]) async {
    Error.throwWithStackTrace(error, stackTrace ?? StackTrace.current);
  }

  @override
  Future<void> retry({Duration? delay}) async {
    completed = true;
    final retryDelay = delay ?? Duration.zero;
    unawaited(
      Future<void>.delayed(retryDelay, () async {
        await _queue.enqueue(
          topic,
          _job.payload,
          headers: headers.incrementAttempt(),
        );
      }),
    );
  }
}
