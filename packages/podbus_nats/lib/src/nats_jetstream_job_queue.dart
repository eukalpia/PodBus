// ignore_for_file: prefer_initializing_formals

import 'dart:async';
import 'dart:math' as math;

import 'package:podbus_core/podbus_core.dart';

import 'config.dart';
import 'nats_jetstream_adapter.dart';

final class NatsJetStreamJobQueue implements DurableJobQueue {
  NatsJetStreamJobQueue({
    required this.config,
    NatsJetStreamAdapter? jetStreamAdapter,
    MessageCodec? codec,
    IdempotencyStore? idempotencyStore,
    Duration idempotencyTtl = const Duration(hours: 24),
    Duration fetchTimeout = const Duration(seconds: 1),
    int fetchBatchSize = 1,
  }) : _adapter = jetStreamAdapter ?? DartNatsJetStreamAdapter(),
       _codec = codec ?? const JsonMessageCodec(),
       _idempotencyStore = idempotencyStore,
       _idempotencyTtl = idempotencyTtl,
       _fetchTimeout = fetchTimeout,
       _fetchBatchSize = fetchBatchSize {
    if (fetchBatchSize < 1) {
      throw const MessagingConfigurationException(
        'NATS JetStream fetch batch size must be greater than zero.',
      );
    }
  }

  static const _contentTypeHeader = 'podbus-content-type';
  static const _schemaVersionHeader = 'podbus-schema-version';
  static const _deadLetterSourceHeader = 'podbus-dead-letter-source';
  static const _deadLetterErrorHeader = 'podbus-dead-letter-error';
  static const _deadLetterStackTraceHeader = 'podbus-dead-letter-stack-trace';
  static const _retryMaxAttemptsHeader = 'podbus-retry-max-attempts';
  static const _retryInitialDelayMicrosHeader =
      'podbus-retry-initial-delay-micros';
  static const _retryMaxDelayMicrosHeader = 'podbus-retry-max-delay-micros';
  static const _retryBackoffMultiplierHeader =
      'podbus-retry-backoff-multiplier';
  static const _retryJitterHeader = 'podbus-retry-jitter';

  final NatsMessagingConfig config;
  final NatsJetStreamAdapter _adapter;
  final MessageCodec _codec;
  final IdempotencyStore? _idempotencyStore;
  final Duration _idempotencyTtl;
  final Duration _fetchTimeout;
  final int _fetchBatchSize;
  final List<_NatsJetStreamWorker<Object?>> _workers = [];
  var _connected = false;

  @override
  Future<void> connect() async {
    final jetStream = _requireJetStreamConfig();
    _validateJetStreamConfig(jetStream);

    await _adapter.connect(config);
    await _adapter.createOrUpdateStream(jetStream);
    _connected = true;
  }

  @override
  Future<void> close({Duration? timeout}) async {
    _connected = false;

    final futures = [for (final worker in _workers.toList()) worker.close()];
    if (timeout == null) {
      await Future.wait(futures);
    } else {
      await Future.wait(futures).timeout(timeout);
    }
    _workers.clear();

    await _adapter.drain();
    await _adapter.close();
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
    _ensureSchedulable(runAt);

    final effectiveHeaders = _withRetryPolicy(
      (headers ?? MessageHeaders()).copyWith(idempotencyKey: idempotencyKey),
      retryPolicy,
    );
    final key = idempotencyKey ?? effectiveHeaders.idempotencyKey;
    var claimedKey = false;
    if (key != null && _idempotencyStore != null) {
      final claimed = await _idempotencyStore.claim(key, ttl: _idempotencyTtl);
      if (!claimed) {
        return;
      }
      claimedKey = true;
    }

    try {
      final encoded = await _codec.encode(payload);
      await _adapter.publish(
        topic,
        encoded.bytes,
        timeout: config.requestTimeout,
        messageId: key,
        headers: _headersFor(effectiveHeaders, encoded),
      );
    } on Object catch (error, stackTrace) {
      if (claimedKey && key != null) {
        await _idempotencyStore!.release(key);
      }
      Error.throwWithStackTrace(error, stackTrace);
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

    final streamName = _requireJetStreamConfig().streamName;
    final consumerName =
        durableName ?? queueGroup ?? _defaultConsumerName(topic);
    final consumer = await _adapter.createOrUpdateConsumer(
      streamName: streamName,
      consumerName: consumerName,
      topic: topic,
    );
    final worker = _NatsJetStreamWorker<T>(
      topic: topic,
      consumerName: consumerName,
      concurrency: concurrency,
      retryPolicy: retryPolicy,
      deadLetterPolicy: deadLetterPolicy ?? const DeadLetterPolicy.disabled(),
      fetchTimeout: _fetchTimeout,
      fetchBatchSize: _fetchBatchSize,
      consumer: consumer,
      queue: this,
      handler: handler,
      onClose: (_NatsJetStreamWorker<Object?> worker) {
        _workers.remove(worker);
      },
    );
    _workers.add(worker as _NatsJetStreamWorker<Object?>);
    worker.start();
    return worker;
  }

  @override
  Future<HealthCheckResult> healthCheck() async {
    if (!_connected || !_adapter.isConnected) {
      return HealthCheckResult.unhealthy(
        message: 'NATS JetStream queue is not connected.',
      );
    }

    try {
      await _adapter.flush();
      return HealthCheckResult.healthy(
        message: 'NATS JetStream queue is connected.',
        details: {'workers': _workers.length},
      );
    } on Object catch (error, stackTrace) {
      return HealthCheckResult(
        status: HealthStatus.unhealthy,
        checkedAt: DateTime.now(),
        message: 'NATS JetStream health check failed.',
        details: {
          'error': error.toString(),
          'stackTrace': stackTrace.toString(),
        },
      );
    }
  }

  Future<T> _decode<T>(NatsJetStreamMessage message) {
    return _codec.decode<T>(
      EncodedMessage(
        bytes: message.bytes,
        contentType:
            message.headers[_contentTypeHeader] ?? JsonMessageCodec.contentType,
        schemaVersion:
            int.tryParse(message.headers[_schemaVersionHeader] ?? '') ?? 1,
      ),
    );
  }

  Future<void> _publishDeadLetter(
    _NatsJetStreamWorker<Object?> worker,
    NatsJetStreamMessage message,
    MessageHeaders headers, {
    Object? error,
    StackTrace? stackTrace,
  }) async {
    final policy = worker.deadLetterPolicy;
    if (!policy.enabled) {
      return;
    }

    final destination = policy.destination ?? '${worker.topic}.dead-letter';
    final customHeaders = {
      ...headers.custom,
      _deadLetterSourceHeader: worker.topic,
      if (policy.includeErrorDetails && error != null)
        _deadLetterErrorHeader: error.toString(),
      if (policy.includeErrorDetails && stackTrace != null)
        _deadLetterStackTraceHeader: stackTrace.toString(),
    };
    final deadLetterHeaders = headers.copyWith(custom: customHeaders);

    await _adapter.publish(
      destination,
      message.bytes,
      timeout: config.requestTimeout,
      headers: {
        ...message.headers,
        for (final MapEntry(:key, :value) in deadLetterHeaders.toMap().entries)
          if (value != null) key: value.toString(),
      },
    );
  }

  Future<void> _handleFailure(
    _NatsJetStreamWorker<Object?> worker,
    NatsJetStreamMessage message,
    MessageHeaders headers,
    int attempt,
    RetryPolicy retryPolicy,
    Object error,
    StackTrace stackTrace,
  ) async {
    if (attempt < retryPolicy.maxAttempts) {
      await _ensureAckAction(
        message.nak(delay: retryPolicy.delayForAttempt(attempt)),
        'nak',
      );
      return;
    }

    await _publishDeadLetter(
      worker,
      message,
      headers.copyWith(attempt: attempt),
      error: error,
      stackTrace: stackTrace,
    );
    await _ensureAckAction(message.term(), 'term');
  }

  MessageHeaders _headersFromMessage(NatsJetStreamMessage message) {
    final headers = MessageHeaders.fromMap(message.headers);
    return headers.copyWith(
      attempt: math.max(headers.attempt, message.deliveryCount),
    );
  }

  RetryPolicy _retryPolicyFor(
    _NatsJetStreamWorker<Object?> worker,
    MessageHeaders headers,
  ) {
    return worker.retryPolicy ??
        _retryPolicyFromHeaders(headers) ??
        RetryPolicy(
          maxAttempts: 1,
          initialDelay: Duration.zero,
          maxDelay: Duration.zero,
        );
  }

  NatsJetStreamConfig _requireJetStreamConfig() {
    final jetStream = config.jetStream;
    if (jetStream == null || !jetStream.enabled) {
      throw const MessagingConfigurationException(
        'NATS JetStream job queue requires an enabled JetStream config.',
      );
    }
    return jetStream;
  }

  void _validateJetStreamConfig(NatsJetStreamConfig jetStream) {
    if (jetStream.streamName.trim().isEmpty) {
      throw const MessagingConfigurationException(
        'NATS JetStream streamName cannot be empty.',
      );
    }
    if (jetStream.subjects.isEmpty) {
      throw const MessagingConfigurationException(
        'NATS JetStream subjects cannot be empty.',
      );
    }
  }

  void _ensureConnected() {
    if (!_connected) {
      throw const MessagingConnectionException(
        'NATS JetStream queue is not connected.',
      );
    }
  }

  void _ensureSchedulable(DateTime? runAt) {
    if (runAt == null || !runAt.isAfter(DateTime.now())) {
      return;
    }

    throw const MessagingUnsupportedException(
      'NATS JetStream does not support durable scheduled enqueue.',
    );
  }

  Map<String, String> _headersFor(
    MessageHeaders headers,
    EncodedMessage encoded,
  ) {
    return {
      for (final MapEntry(:key, :value) in headers.toMap().entries)
        if (value != null) key: value.toString(),
      _contentTypeHeader: encoded.contentType,
      _schemaVersionHeader: encoded.schemaVersion.toString(),
    };
  }

  MessageHeaders _withRetryPolicy(
    MessageHeaders headers,
    RetryPolicy? retryPolicy,
  ) {
    if (retryPolicy == null) {
      return headers;
    }

    return headers.copyWith(
      custom: {
        ...headers.custom,
        _retryMaxAttemptsHeader: retryPolicy.maxAttempts.toString(),
        _retryInitialDelayMicrosHeader: retryPolicy.initialDelay.inMicroseconds
            .toString(),
        _retryMaxDelayMicrosHeader: retryPolicy.maxDelay.inMicroseconds
            .toString(),
        _retryBackoffMultiplierHeader: retryPolicy.backoffMultiplier.toString(),
        _retryJitterHeader: retryPolicy.jitter.toString(),
      },
    );
  }

  RetryPolicy? _retryPolicyFromHeaders(MessageHeaders headers) {
    final maxAttempts = int.tryParse(
      headers.custom[_retryMaxAttemptsHeader] ?? '',
    );
    final initialDelayMicros = int.tryParse(
      headers.custom[_retryInitialDelayMicrosHeader] ?? '',
    );
    final maxDelayMicros = int.tryParse(
      headers.custom[_retryMaxDelayMicrosHeader] ?? '',
    );
    final backoffMultiplier = double.tryParse(
      headers.custom[_retryBackoffMultiplierHeader] ?? '',
    );
    final jitter = double.tryParse(headers.custom[_retryJitterHeader] ?? '');

    if (maxAttempts == null ||
        initialDelayMicros == null ||
        maxDelayMicros == null) {
      return null;
    }

    return RetryPolicy(
      maxAttempts: maxAttempts,
      initialDelay: Duration(microseconds: initialDelayMicros),
      maxDelay: Duration(microseconds: maxDelayMicros),
      backoffMultiplier: backoffMultiplier ?? 2,
      jitter: jitter ?? 0,
    );
  }

  String _defaultConsumerName(String topic) {
    final buffer = StringBuffer('podbus_');
    for (final unit in topic.codeUnits) {
      final char = String.fromCharCode(unit);
      final allowed =
          (unit >= 48 && unit <= 57) ||
          (unit >= 65 && unit <= 90) ||
          (unit >= 97 && unit <= 122);
      buffer.write(allowed ? char : '_');
    }
    return buffer.toString();
  }
}

final class _NatsJetStreamWorker<T> implements Worker {
  _NatsJetStreamWorker({
    required this.topic,
    required this.consumerName,
    required this.concurrency,
    required this.retryPolicy,
    required this.deadLetterPolicy,
    required this.fetchTimeout,
    required this.fetchBatchSize,
    required this.consumer,
    required this.queue,
    required this.handler,
    required this.onClose,
  });

  final String topic;
  final String consumerName;
  final int concurrency;
  final RetryPolicy? retryPolicy;
  final DeadLetterPolicy deadLetterPolicy;
  final Duration fetchTimeout;
  final int fetchBatchSize;
  final NatsJetStreamConsumer consumer;
  final NatsJetStreamJobQueue queue;
  final Future<void> Function(JobContext context, T payload) handler;
  final void Function(_NatsJetStreamWorker<Object?> worker) onClose;
  final List<Future<void>> _tasks = [];
  Object? lastError;
  StackTrace? lastStackTrace;
  var _closed = false;

  void start() {
    for (var i = 0; i < concurrency; i += 1) {
      _tasks.add(_run());
    }
  }

  Future<void> _run() async {
    while (!_closed) {
      try {
        final messages = await consumer.fetch(
          batch: fetchBatchSize,
          timeout: fetchTimeout,
        );
        if (messages.isEmpty) {
          continue;
        }
        for (final message in messages) {
          if (_closed) {
            return;
          }
          await _process(message);
        }
      } on Object catch (error, stackTrace) {
        lastError = error;
        lastStackTrace = stackTrace;
        await Future<void>.delayed(const Duration(milliseconds: 200));
      }
    }
  }

  Future<void> _process(NatsJetStreamMessage message) async {
    final headers = queue._headersFromMessage(message);
    final retryPolicy = queue._retryPolicyFor(
      this as _NatsJetStreamWorker<Object?>,
      headers,
    );
    final attempt = math.max(headers.attempt, message.deliveryCount);
    final context = _NatsJetStreamJobContext(
      topic: topic,
      headers: headers.copyWith(attempt: attempt),
      rawMessage: message,
      attempt: attempt,
      maxAttempts: retryPolicy.maxAttempts,
      message: message,
      queue: queue,
      worker: this as _NatsJetStreamWorker<Object?>,
    );

    try {
      final payload = await queue._decode<T>(message);
      await handler(context, payload);
      if (!context.completed) {
        await context.ack();
      }
    } on Object catch (error, stackTrace) {
      await queue._handleFailure(
        this as _NatsJetStreamWorker<Object?>,
        message,
        headers,
        attempt,
        retryPolicy,
        error,
        stackTrace,
      );
    }
  }

  @override
  Future<void> close() async {
    if (_closed) {
      return;
    }
    _closed = true;
    await Future.wait(_tasks);
    onClose(this as _NatsJetStreamWorker<Object?>);
  }
}

final class _NatsJetStreamJobContext implements JobContext {
  _NatsJetStreamJobContext({
    required this.topic,
    required this.headers,
    required this.rawMessage,
    required this.attempt,
    required this.maxAttempts,
    required this.message,
    required this.queue,
    required this.worker,
  });

  final NatsJetStreamMessage message;
  final NatsJetStreamJobQueue queue;
  final _NatsJetStreamWorker<Object?> worker;
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
    await _ensureAckAction(message.ack(), 'ack');
    completed = true;
  }

  @override
  Future<void> deadLetter({Object? error, StackTrace? stackTrace}) async {
    await queue._publishDeadLetter(
      worker,
      message,
      headers,
      error: error,
      stackTrace: stackTrace,
    );
    await _ensureAckAction(message.term(), 'term');
    completed = true;
  }

  @override
  Future<void> fail(Object error, [StackTrace? stackTrace]) async {
    Error.throwWithStackTrace(error, stackTrace ?? StackTrace.current);
  }

  @override
  Future<void> retry({Duration? delay}) async {
    await _ensureAckAction(message.nak(delay: delay), 'nak');
    completed = true;
  }
}

Future<void> _ensureAckAction(Future<bool> action, String name) async {
  final sent = await action;
  if (!sent) {
    throw MessagingConnectionException(
      'NATS JetStream message $name failed because no ack subject was present.',
    );
  }
}
