// ignore_for_file: prefer_initializing_formals

import 'dart:async';
import 'dart:collection';

import 'package:podbus_core/podbus_core.dart';

import 'config.dart';
import 'rabbitmq_adapter.dart';

final class RabbitMqMessageBus implements MessageBus, DurableJobQueue {
  RabbitMqMessageBus({
    required this.config,
    RabbitMqAdapter? adapter,
    MessageCodec? codec,
    IdempotencyStore? idempotencyStore,
    Duration idempotencyTtl = const Duration(hours: 24),
  }) : _adapter = adapter ?? DartRabbitMqAdapter(),
       _codec = codec ?? const JsonMessageCodec(),
       _idempotencyStore = idempotencyStore,
       _idempotencyTtl = idempotencyTtl;

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

  final RabbitMqMessagingConfig config;
  final RabbitMqAdapter _adapter;
  final MessageCodec _codec;
  final IdempotencyStore? _idempotencyStore;
  final Duration _idempotencyTtl;
  final List<_RabbitMqSubscription> _subscriptions = [];
  final List<_RabbitMqWorker<Object?>> _workers = [];
  Object? _lastWorkerError;
  DateTime? _lastWorkerErrorAt;
  var _connected = false;

  @override
  Future<void> connect() async {
    await _adapter.connect(config);
    await _adapter.declareExchange(
      name: config.exchange,
      durable: config.durable,
    );
    await _adapter.declareExchange(
      name: config.deadLetterExchange,
      durable: config.durable,
    );
    _connected = true;
  }

  @override
  Future<void> close({Duration? timeout}) async {
    _connected = false;

    final futures = [
      for (final subscription in _subscriptions.toList()) subscription.close(),
      for (final worker in _workers.toList()) worker.close(),
    ];
    if (timeout == null) {
      await Future.wait(futures);
    } else {
      await Future.wait(futures).timeout(timeout);
    }

    _subscriptions.clear();
    _workers.clear();
    await _adapter.close();
  }

  @override
  Future<void> publish<T>(
    String subject,
    T payload, {
    MessageHeaders? headers,
  }) async {
    _ensureConnected();
    await _publishEncoded(
      exchange: config.exchange,
      routingKey: subject,
      payload: payload,
      headers: headers ?? MessageHeaders(),
    );
  }

  @override
  Future<Subscription> subscribe<T>(
    String subject, {
    String? queueGroup,
    required MessageHandler<T> handler,
  }) async {
    _ensureConnected();

    final queue = queueGroup == null
        ? _ephemeralQueueName(subject)
        : _namedQueue('events', subject, queueGroup);
    await _declareBoundQueue(queue: queue, routingKey: subject);
    await _adapter.setPrefetchCount(config.prefetchCount);
    final consumer = await _adapter.consume(queue: queue, noAck: false);

    late final _RabbitMqSubscription subscription;
    subscription = _RabbitMqSubscription(
      consumer: consumer,
      onClose: () => _subscriptions.remove(subscription),
    );
    _subscriptions.add(subscription);
    subscription.listen((delivery) async {
      await _handleEventDelivery(subject, delivery, handler);
    });
    return subscription;
  }

  @override
  Future<TResponse> request<TRequest, TResponse>(
    String subject,
    TRequest payload, {
    MessageHeaders? headers,
    Duration? timeout,
  }) {
    throw const MessagingUnsupportedException(
      'RabbitMQ request/reply is not implemented yet.',
    );
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

    final queue = _jobQueueName(topic);
    await _declareBoundQueue(queue: queue, routingKey: topic);

    final effectiveHeaders = _withRetryPolicy(
      (headers ?? MessageHeaders()).copyWith(idempotencyKey: idempotencyKey),
      retryPolicy,
    );
    final key = idempotencyKey ?? effectiveHeaders.idempotencyKey;
    if (key != null && _idempotencyStore != null) {
      final claimed = await _idempotencyStore.claim(key, ttl: _idempotencyTtl);
      if (!claimed) {
        return;
      }
    }

    await _publishEncoded(
      exchange: config.exchange,
      routingKey: topic,
      payload: payload,
      headers: effectiveHeaders,
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
    _ensureConnected();
    if (concurrency < 1) {
      throw const MessagingConfigurationException(
        'Worker concurrency must be greater than zero.',
      );
    }

    final queue = durableName ?? queueGroup ?? _jobQueueName(topic);
    await _declareBoundQueue(queue: queue, routingKey: topic);
    await _adapter.setPrefetchCount(config.prefetchCount);
    final consumer = await _adapter.consume(queue: queue, noAck: false);
    final worker = _RabbitMqWorker<T>(
      topic: topic,
      consumer: consumer,
      concurrency: concurrency,
      retryPolicy: retryPolicy,
      deadLetterPolicy: deadLetterPolicy ?? const DeadLetterPolicy.disabled(),
      bus: this,
      handler: handler,
      onClose: (_RabbitMqWorker<Object?> worker) {
        _workers.remove(worker);
      },
    );
    _workers.add(worker as _RabbitMqWorker<Object?>);
    worker.start();
    return worker;
  }

  @override
  Future<HealthCheckResult> healthCheck() async {
    if (!_connected || !_adapter.isConnected) {
      return HealthCheckResult.unhealthy(
        message: 'RabbitMQ adapter is not connected.',
      );
    }
    final lastWorkerError = _lastWorkerError;
    if (lastWorkerError != null) {
      return HealthCheckResult.unhealthy(
        message: 'RabbitMQ worker failure handling failed.',
        details: {
          'lastWorkerError': lastWorkerError.toString(),
          if (_lastWorkerErrorAt != null)
            'lastWorkerErrorAt': _lastWorkerErrorAt!.toIso8601String(),
          'subscriptions': _subscriptions.length,
          'workers': _workers.length,
        },
      );
    }
    return HealthCheckResult.healthy(
      message: 'RabbitMQ adapter is connected.',
      details: {
        'subscriptions': _subscriptions.length,
        'workers': _workers.length,
      },
    );
  }

  void _recordWorkerError(Object error) {
    _lastWorkerError = error;
    _lastWorkerErrorAt = DateTime.now();
  }

  Future<void> _handleEventDelivery<T>(
    String subject,
    RabbitMqDelivery delivery,
    MessageHandler<T> handler,
  ) async {
    _RabbitMqMessageContext? context;
    try {
      context = _RabbitMqMessageContext(
        subject: subject,
        headers: MessageHeaders.fromMap(delivery.headers),
        rawMessage: delivery,
        delivery: delivery,
      );
      final payload = await _decode<T>(delivery);
      await handler(context, payload);
      if (!context.completed) {
        await context.ack();
      }
    } on Object {
      if (context?.completed != true) {
        await delivery.nack(requeue: false);
      }
    }
  }

  Future<T> _decode<T>(RabbitMqDelivery delivery) {
    return _codec.decode<T>(
      EncodedMessage(
        bytes: delivery.bytes,
        contentType:
            delivery.headers[_contentTypeHeader] ??
            JsonMessageCodec.contentType,
        schemaVersion:
            int.tryParse(delivery.headers[_schemaVersionHeader] ?? '') ?? 1,
      ),
    );
  }

  Future<void> _handleJobFailure(
    _RabbitMqWorker<Object?> worker,
    RabbitMqDelivery delivery,
    MessageHeaders headers,
    int attempt,
    RetryPolicy retryPolicy,
    Object error,
    StackTrace stackTrace,
  ) async {
    if (attempt < retryPolicy.maxAttempts) {
      final delay = retryPolicy.delayForAttempt(attempt);
      if (delay > Duration.zero) {
        await Future<void>.delayed(delay);
      }
      await _publishRaw(
        exchange: config.exchange,
        routingKey: worker.topic,
        bytes: delivery.bytes,
        headers: _rawHeadersWithAttempt(delivery.headers, attempt + 1),
      );
      await delivery.ack();
      return;
    }

    if (worker.deadLetterPolicy.enabled) {
      await _publishDeadLetter(
        worker,
        delivery,
        headers.copyWith(attempt: attempt),
        error: error,
        stackTrace: stackTrace,
      );
      await delivery.ack();
      return;
    }

    await delivery.nack(requeue: false);
  }

  Future<void> _handleMalformedJob(
    _RabbitMqWorker<Object?> worker,
    RabbitMqDelivery delivery,
    Object error,
    StackTrace stackTrace,
  ) async {
    if (worker.deadLetterPolicy.enabled) {
      await _publishDeadLetter(
        worker,
        delivery,
        MessageHeaders(),
        error: error,
        stackTrace: stackTrace,
      );
      await delivery.ack();
      return;
    }

    await delivery.nack(requeue: false);
  }

  Future<void> _publishDeadLetter(
    _RabbitMqWorker<Object?> worker,
    RabbitMqDelivery delivery,
    MessageHeaders headers, {
    Object? error,
    StackTrace? stackTrace,
  }) async {
    final policy = worker.deadLetterPolicy;
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
    await _publishRaw(
      exchange: config.deadLetterExchange,
      routingKey: destination,
      bytes: delivery.bytes,
      headers: {
        ...delivery.headers,
        for (final MapEntry(:key, :value) in deadLetterHeaders.toMap().entries)
          if (value != null) key: value.toString(),
      },
    );
  }

  Future<void> _publishEncoded<T>({
    required String exchange,
    required String routingKey,
    required T payload,
    required MessageHeaders headers,
  }) async {
    final encoded = await _codec.encode(payload);
    await _publishRaw(
      exchange: exchange,
      routingKey: routingKey,
      bytes: encoded.bytes,
      headers: _headersFor(headers, encoded),
    );
  }

  Future<void> _publishRaw({
    required String exchange,
    required String routingKey,
    required List<int> bytes,
    required Map<String, String> headers,
  }) {
    return _adapter.publish(
      exchange: exchange,
      routingKey: routingKey,
      bytes: bytes,
      headers: headers,
      persistent: config.durable,
    );
  }

  Future<void> _declareBoundQueue({
    required String queue,
    required String routingKey,
  }) async {
    await _adapter.declareQueue(
      name: queue,
      durable: config.durable,
      arguments: {'x-dead-letter-exchange': config.deadLetterExchange},
    );
    await _adapter.bindQueue(
      queue: queue,
      exchange: config.exchange,
      routingKey: routingKey,
    );
  }

  RetryPolicy _retryPolicyFor(
    _RabbitMqWorker<Object?> worker,
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

  Map<String, String> _rawHeadersWithAttempt(
    Map<String, String> headers,
    int attempt,
  ) {
    return {...headers, 'attempt': attempt.toString()};
  }

  String _ephemeralQueueName(String subject) {
    return _namedQueue(
      'events',
      subject,
      DateTime.now().microsecondsSinceEpoch.toString(),
    );
  }

  String _jobQueueName(String topic) => _namedQueue('jobs', topic, 'default');

  String _namedQueue(String kind, String routingKey, String name) {
    return 'podbus.$kind.${_sanitize(name)}.${_sanitize(routingKey)}';
  }

  String _sanitize(String value) {
    final buffer = StringBuffer();
    for (final unit in value.codeUnits) {
      final allowed =
          (unit >= 48 && unit <= 57) ||
          (unit >= 65 && unit <= 90) ||
          (unit >= 97 && unit <= 122);
      buffer.write(allowed ? String.fromCharCode(unit) : '_');
    }
    return buffer.toString();
  }

  void _ensureConnected() {
    if (!_connected) {
      throw const MessagingConnectionException(
        'RabbitMQ adapter is not connected.',
      );
    }
  }

  void _ensureSchedulable(DateTime? runAt) {
    if (runAt == null || !runAt.isAfter(DateTime.now())) {
      return;
    }
    throw const MessagingUnsupportedException(
      'RabbitMQ delayed enqueue requires a delayed-message strategy.',
    );
  }
}

final class _RabbitMqSubscription implements Subscription {
  _RabbitMqSubscription({required this.consumer, required this.onClose});

  final RabbitMqConsumer consumer;
  final void Function() onClose;
  StreamSubscription<RabbitMqDelivery>? _subscription;
  var _closed = false;

  void listen(Future<void> Function(RabbitMqDelivery delivery) onDelivery) {
    _subscription = consumer.deliveries.listen((delivery) {
      unawaited(onDelivery(delivery));
    });
  }

  @override
  Future<void> close() async {
    if (_closed) {
      return;
    }
    _closed = true;
    await _subscription?.cancel();
    await consumer.close();
    onClose();
  }
}

final class _RabbitMqWorker<T> implements Worker {
  _RabbitMqWorker({
    required this.topic,
    required this.consumer,
    required this.concurrency,
    required this.retryPolicy,
    required this.deadLetterPolicy,
    required this.bus,
    required this.handler,
    required this.onClose,
  });

  final String topic;
  final RabbitMqConsumer consumer;
  final int concurrency;
  final RetryPolicy? retryPolicy;
  final DeadLetterPolicy deadLetterPolicy;
  final RabbitMqMessageBus bus;
  final JobHandler<T> handler;
  final void Function(_RabbitMqWorker<Object?> worker) onClose;
  final Queue<RabbitMqDelivery> _pending = Queue();
  StreamSubscription<RabbitMqDelivery>? _subscription;
  var _active = 0;
  var _closed = false;

  void start() {
    _subscription = consumer.deliveries.listen((delivery) {
      _pending.add(delivery);
      _drain();
    });
  }

  void _drain() {
    while (!_closed && _active < concurrency && _pending.isNotEmpty) {
      final delivery = _pending.removeFirst();
      _active += 1;
      unawaited(_process(delivery));
    }
  }

  Future<void> _process(RabbitMqDelivery delivery) async {
    MessageHeaders? headers;
    RetryPolicy? effectiveRetryPolicy;
    try {
      headers = MessageHeaders.fromMap(delivery.headers);
      effectiveRetryPolicy = bus._retryPolicyFor(
        this as _RabbitMqWorker<Object?>,
        headers,
      );
      final context = _RabbitMqJobContext(
        topic: topic,
        headers: headers,
        rawMessage: delivery,
        attempt: headers.attempt,
        maxAttempts: effectiveRetryPolicy.maxAttempts,
        delivery: delivery,
        bus: bus,
        worker: this as _RabbitMqWorker<Object?>,
      );
      final payload = await bus._decode<T>(delivery);
      await handler(context, payload);
      if (!context.completed) {
        await context.ack();
      }
    } on Object catch (error, stackTrace) {
      try {
        final parsedHeaders = headers;
        final parsedRetryPolicy = effectiveRetryPolicy;
        if (parsedHeaders == null || parsedRetryPolicy == null) {
          await bus._handleMalformedJob(
            this as _RabbitMqWorker<Object?>,
            delivery,
            error,
            stackTrace,
          );
        } else {
          await bus._handleJobFailure(
            this as _RabbitMqWorker<Object?>,
            delivery,
            parsedHeaders,
            parsedHeaders.attempt,
            parsedRetryPolicy,
            error,
            stackTrace,
          );
        }
      } on Object catch (failureError) {
        bus._recordWorkerError(failureError);
      }
    } finally {
      _active -= 1;
      _drain();
    }
  }

  @override
  Future<void> close() async {
    if (_closed) {
      return;
    }
    _closed = true;
    await _subscription?.cancel();
    await consumer.close();
    onClose(this as _RabbitMqWorker<Object?>);
  }
}

final class _RabbitMqMessageContext implements MessageContext {
  _RabbitMqMessageContext({
    required this.subject,
    required this.headers,
    required this.rawMessage,
    required this.delivery,
  });

  final RabbitMqDelivery delivery;
  var completed = false;

  @override
  final String subject;

  @override
  final MessageHeaders headers;

  @override
  final Object? rawMessage;

  @override
  Future<void> ack() async {
    await delivery.ack();
    completed = true;
  }

  @override
  Future<void> extendVisibility(Duration duration) {
    throw const MessagingUnsupportedException(
      'RabbitMQ does not support visibility extension.',
    );
  }

  @override
  Future<void> nak({Duration? delay}) async {
    if (delay != null && delay > Duration.zero) {
      throw const MessagingUnsupportedException(
        'RabbitMQ delayed negative acknowledgement requires a retry strategy.',
      );
    }
    await delivery.nack(requeue: true);
    completed = true;
  }

  @override
  Future<void> reply<T>(T payload, {MessageHeaders? headers}) {
    throw const MessagingUnsupportedException(
      'RabbitMQ request/reply is not implemented yet.',
    );
  }

  @override
  Future<void> terminate() async {
    await delivery.nack(requeue: false);
    completed = true;
  }
}

final class _RabbitMqJobContext implements JobContext {
  _RabbitMqJobContext({
    required this.topic,
    required this.headers,
    required this.rawMessage,
    required this.attempt,
    required this.maxAttempts,
    required this.delivery,
    required this.bus,
    required this.worker,
  });

  final RabbitMqDelivery delivery;
  final RabbitMqMessageBus bus;
  final _RabbitMqWorker<Object?> worker;
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
    await delivery.ack();
    completed = true;
  }

  @override
  Future<void> deadLetter({Object? error, StackTrace? stackTrace}) async {
    await bus._publishDeadLetter(
      worker,
      delivery,
      headers,
      error: error,
      stackTrace: stackTrace,
    );
    await delivery.ack();
    completed = true;
  }

  @override
  Future<void> fail(Object error, [StackTrace? stackTrace]) async {
    Error.throwWithStackTrace(error, stackTrace ?? StackTrace.current);
  }

  @override
  Future<void> retry({Duration? delay}) async {
    if (delay != null && delay > Duration.zero) {
      await Future<void>.delayed(delay);
    }
    await bus._publishRaw(
      exchange: bus.config.exchange,
      routingKey: topic,
      bytes: delivery.bytes,
      headers: bus._rawHeadersWithAttempt(delivery.headers, attempt + 1),
    );
    await delivery.ack();
    completed = true;
  }
}
