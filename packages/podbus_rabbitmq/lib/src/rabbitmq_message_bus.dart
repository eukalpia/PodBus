// ignore_for_file: prefer_initializing_formals

import 'dart:async';
import 'dart:collection';

import 'package:podbus_core/podbus_core.dart';

import 'config.dart';
import 'rabbitmq_adapter.dart';

final class RabbitMqMessageBus implements MessageBus, DurableJobQueue {
  RabbitMqMessageBus({
    required this.config,
    MessagingConfig? messagingConfig,
    RabbitMqAdapter? adapter,
    MessageCodec? codec,
    IdempotencyStore? idempotencyStore,
    Duration idempotencyTtl = const Duration(hours: 24),
  }) : messagingConfig = messagingConfig ?? MessagingConfig(codec: codec),
       _adapter = adapter ?? DartRabbitMqAdapter(),
       _idempotencyStore =
           idempotencyStore ?? messagingConfig?.idempotencyStore,
       _idempotencyTtl = idempotencyTtl;

  static const _capabilities = MessagingCapabilities({
    MessagingCapability.publishSubscribe,
    MessagingCapability.queueGroups,
    MessagingCapability.manualAcknowledgement,
    MessagingCapability.negativeAcknowledgement,
    MessagingCapability.termination,
    MessagingCapability.durableJobs,
    MessagingCapability.retries,
    MessagingCapability.deadLettering,
    MessagingCapability.idempotentPublish,
    MessagingCapability.typedPayloads,
    MessagingCapability.gracefulShutdown,
  });

  final RabbitMqMessagingConfig config;
  final MessagingConfig messagingConfig;
  final RabbitMqAdapter _adapter;
  final IdempotencyStore? _idempotencyStore;
  final Duration _idempotencyTtl;
  final List<_RabbitMqSubscription> _subscriptions = [];
  final List<_RabbitMqWorker<Object?>> _workers = [];
  Object? _lastWorkerError;
  DateTime? _lastWorkerErrorAt;
  var _connected = false;
  var _closing = false;
  Future<void>? _closeFuture;

  MessageCodec get _codec => messagingConfig.codec;

  @override
  MessagingCapabilities get capabilities => _capabilities;

  @override
  Future<void> connect() async {
    _closing = false;
    try {
      await _adapter.connect(config);
      await _adapter.declareExchange(
        name: config.exchange,
        durable: config.durable,
      );
      await _adapter.declareExchange(
        name: config.deadLetterExchange,
        durable: config.durable,
      );
      if (config.useBrokerRetryQueues) {
        await _adapter.declareExchange(
          name: config.effectiveRetryExchange,
          durable: config.durable,
        );
      }
      _connected = true;
    } on Object catch (error, stackTrace) {
      try {
        await _adapter.close();
      } on Object {
        // Preserve the startup failure.
      }
      Error.throwWithStackTrace(error, stackTrace);
    }
  }

  @override
  Future<void> close({Duration? timeout}) {
    final existing = _closeFuture;
    if (existing != null) return existing;
    final effectiveTimeout = timeout ?? messagingConfig.shutdownTimeout;
    late final Future<void> closing;
    closing = _closeResources(effectiveTimeout).whenComplete(() {
      if (identical(_closeFuture, closing)) _closeFuture = null;
    });
    _closeFuture = closing;
    return closing;
  }

  Future<void> _closeResources(Duration effectiveTimeout) async {
    _closing = true;
    _connected = false;
    final subscriptions = _subscriptions.toList();
    final workers = _workers.toList();
    _subscriptions.clear();
    _workers.clear();
    Object? failure;
    StackTrace? failureStackTrace;
    Future<void> guard(Future<void> Function() action) async {
      try {
        await action().timeout(effectiveTimeout);
      } on Object catch (e, s) {
        failure ??= e;
        failureStackTrace ??= s;
      }
    }

    try {
      await Future.wait([
        guard(_adapter.close),
        guard(
          () => Future.wait([
            for (final subscription in subscriptions) subscription.close(),
            for (final worker in workers) worker.close(),
          ]),
        ),
      ]);
    } finally {
      _closing = false;
    }
    if (failure != null)
      Error.throwWithStackTrace(failure!, failureStackTrace!);
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
    int concurrency = 1,
    required MessageHandler<T> handler,
  }) async {
    _ensureConnected();
    if (concurrency < 1) {
      throw const MessagingConfigurationException(
        'Subscription concurrency must be greater than zero.',
      );
    }

    final queue = queueGroup == null
        ? _ephemeralQueueName(subject)
        : _namedQueue('events', subject, queueGroup);
    await _declareBoundQueue(
      queue: queue,
      routingKey: subject,
      ephemeral: queueGroup == null,
    );
    await _adapter.setPrefetchCount(config.prefetchCount);
    final consumer = await _adapter.consume(queue: queue, noAck: false);

    late final _RabbitMqSubscription subscription;
    subscription = _RabbitMqSubscription(
      consumer: consumer,
      concurrency: concurrency,
      messagingConfig: messagingConfig,
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
    var claimed = false;
    if (key != null && _idempotencyStore != null) {
      claimed = await _idempotencyStore.claim(key, ttl: _idempotencyTtl);
      if (!claimed) {
        return;
      }
    }

    try {
      await _publishEncoded(
        exchange: config.exchange,
        routingKey: topic,
        payload: payload,
        headers: effectiveHeaders,
      );
    } on Object catch (error, stackTrace) {
      if (claimed && key != null && _idempotencyStore != null) {
        await _idempotencyStore.release(key);
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
        message: 'RabbitMQ is connected, but worker failure handling failed.',
        details: {
          'lastWorkerError': messagingConfig.limits.truncateError(
            lastWorkerError,
          ),
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
    _lastWorkerErrorAt = messagingConfig.now();
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
    } on Object catch (error, stackTrace) {
      messagingConfig.log(
        MessagingLogLevel.error,
        'RabbitMQ event handler failed.',
        error: error,
        stackTrace: stackTrace,
        attributes: {'transport': 'rabbitmq', 'subject': subject},
      );
      if (context?.completed != true) {
        await delivery.nack(requeue: false);
      }
    }
  }

  Future<T> _decode<T>(RabbitMqDelivery delivery) {
    messagingConfig.limits.validatePayload(delivery.bytes);
    messagingConfig.limits.validateHeaders(delivery.headers);
    return _codec.decode<T>(
      EncodedMessage(
        bytes: delivery.bytes,
        contentType:
            delivery.headers[PodBusWireHeaders.contentType] ??
            JsonMessageCodec.contentType,
        schemaVersion:
            int.tryParse(
              delivery.headers[PodBusWireHeaders.schemaVersion] ?? '',
            ) ??
            1,
        messageType: delivery.headers[PodBusWireHeaders.messageType],
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
    if (messagingConfig.shouldRetry(error) &&
        attempt < retryPolicy.maxAttempts) {
      final delay = retryPolicy.delayForAttempt(attempt);
      if (config.useBrokerRetryQueues && delay > Duration.zero) {
        await _publishRetry(
          worker: worker,
          delivery: delivery,
          attempt: attempt + 1,
          delay: delay,
        );
      } else {
        if (delay > Duration.zero) {
          await Future<void>.delayed(delay);
        }
        await _publishRaw(
          exchange: config.exchange,
          routingKey: worker.topic,
          bytes: delivery.bytes,
          headers: _rawHeadersWithAttempt(delivery.headers, attempt + 1),
        );
      }
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

  Future<void> _publishRetry({
    required _RabbitMqWorker<Object?> worker,
    required RabbitMqDelivery delivery,
    required int attempt,
    required Duration delay,
  }) async {
    final delayMs = delay.inMilliseconds.clamp(1, 2147483647).toInt();
    final routingKey = '${worker.topic}.retry.$delayMs';
    final queue = _namedQueue('retry', worker.topic, '${delayMs}ms');
    await _adapter.declareQueue(
      name: queue,
      durable: config.durable,
      arguments: {
        'x-message-ttl': delayMs,
        'x-dead-letter-exchange': config.exchange,
        'x-dead-letter-routing-key': worker.topic,
      },
    );
    await _adapter.bindQueue(
      queue: queue,
      exchange: config.effectiveRetryExchange,
      routingKey: routingKey,
    );
    await _publishRaw(
      exchange: config.effectiveRetryExchange,
      routingKey: routingKey,
      bytes: delivery.bytes,
      headers: _rawHeadersWithAttempt(delivery.headers, attempt),
    );
    messagingConfig.recordMetric(
      'podbus.jobs.retried',
      attributes: {
        'transport': 'rabbitmq',
        'topic': worker.topic,
        'delayMs': delayMs,
      },
    );
  }

  Future<void> _publishDeadLetter(
    _RabbitMqWorker<Object?> worker,
    RabbitMqDelivery delivery,
    MessageHeaders headers, {
    Object? error,
    StackTrace? stackTrace,
  }) async {
    final policy = worker.deadLetterPolicy;
    if (!policy.enabled) {
      return;
    }

    final destination = policy.destination ?? '${worker.topic}.dead-letter';
    final customHeaders = <String, String>{
      ...headers.custom,
      PodBusWireHeaders.deadLetterSource: worker.topic,
      if (!policy.includeOriginalPayload)
        PodBusWireHeaders.deadLetterPayloadOmitted: 'true',
      if (policy.includeErrorDetails && error != null)
        PodBusWireHeaders.deadLetterError: messagingConfig.limits.truncateError(
          error,
        ),
      if (policy.includeErrorDetails && stackTrace != null)
        PodBusWireHeaders.deadLetterStackTrace: messagingConfig.limits
            .truncateError(stackTrace),
    };
    final deadLetterHeaders = headers.withoutIdempotencyKey().copyWith(
      custom: customHeaders,
    );
    final bytes = policy.includeOriginalPayload
        ? delivery.bytes
        : const <int>[];
    final wireHeaders = <String, String>{
      for (final MapEntry(:key, :value) in deadLetterHeaders.toMap().entries)
        if (value != null) key: value.toString(),
      PodBusWireHeaders.contentType: policy.includeOriginalPayload
          ? (delivery.headers[PodBusWireHeaders.contentType] ??
                JsonMessageCodec.contentType)
          : 'application/octet-stream',
      PodBusWireHeaders.schemaVersion: policy.includeOriginalPayload
          ? (delivery.headers[PodBusWireHeaders.schemaVersion] ?? '1')
          : '1',
      if (policy.includeOriginalPayload &&
          delivery.headers[PodBusWireHeaders.messageType] != null)
        PodBusWireHeaders.messageType:
            delivery.headers[PodBusWireHeaders.messageType]!,
    };

    await _publishRaw(
      exchange: config.deadLetterExchange,
      routingKey: destination,
      bytes: bytes,
      headers: wireHeaders,
    );
    messagingConfig.recordMetric(
      'podbus.jobs.dead_lettered',
      attributes: {'transport': 'rabbitmq', 'topic': worker.topic},
    );
  }

  Future<void> _publishEncoded<T>({
    required String exchange,
    required String routingKey,
    required T payload,
    required MessageHeaders headers,
  }) async {
    final encoded = await _codec.encode(payload);
    final wireHeaders = _headersFor(headers, encoded);
    messagingConfig.validateRawOutbound(encoded.bytes, wireHeaders);
    await _publishRaw(
      exchange: exchange,
      routingKey: routingKey,
      bytes: encoded.bytes,
      headers: wireHeaders,
    );
    messagingConfig.recordMetric(
      'podbus.messages.published',
      attributes: {
        'transport': 'rabbitmq',
        'exchange': exchange,
        'routingKey': routingKey,
      },
    );
  }

  Future<void> _publishRaw({
    required String exchange,
    required String routingKey,
    required List<int> bytes,
    required Map<String, String> headers,
  }) {
    messagingConfig.validateRawOutbound(bytes, headers);
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
    bool ephemeral = false,
  }) async {
    await _adapter.declareQueue(
      name: queue,
      durable: ephemeral ? false : config.durable,
      exclusive: ephemeral,
      autoDelete: ephemeral,
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
        messagingConfig.defaultRetryPolicy;
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
        PodBusWireHeaders.retryMaxAttempts: retryPolicy.maxAttempts.toString(),
        PodBusWireHeaders.retryInitialDelayMicros: retryPolicy
            .initialDelay
            .inMicroseconds
            .toString(),
        PodBusWireHeaders.retryMaxDelayMicros: retryPolicy
            .maxDelay
            .inMicroseconds
            .toString(),
        PodBusWireHeaders.retryBackoffMultiplier: retryPolicy.backoffMultiplier
            .toString(),
        PodBusWireHeaders.retryJitter: retryPolicy.jitter.toString(),
      },
    );
  }

  RetryPolicy? _retryPolicyFromHeaders(MessageHeaders headers) {
    final maxAttempts = int.tryParse(
      headers.custom[PodBusWireHeaders.retryMaxAttempts] ?? '',
    );
    final initialDelayMicros = int.tryParse(
      headers.custom[PodBusWireHeaders.retryInitialDelayMicros] ?? '',
    );
    final maxDelayMicros = int.tryParse(
      headers.custom[PodBusWireHeaders.retryMaxDelayMicros] ?? '',
    );
    final backoffMultiplier = double.tryParse(
      headers.custom[PodBusWireHeaders.retryBackoffMultiplier] ?? '',
    );
    final jitter = double.tryParse(
      headers.custom[PodBusWireHeaders.retryJitter] ?? '',
    );

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
      PodBusWireHeaders.contentType: encoded.contentType,
      PodBusWireHeaders.schemaVersion: encoded.schemaVersion.toString(),
      if (encoded.messageType != null)
        PodBusWireHeaders.messageType: encoded.messageType!,
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
      messagingConfig.now().microsecondsSinceEpoch.toString(),
    );
  }

  String _jobQueueName(String topic) => _namedQueue('jobs', topic, 'default');

  String _namedQueue(String kind, String routingKey, String name) {
    return 'podbus.$kind.${_sanitize(name)}.${_sanitize(routingKey)}';
  }

  String _sanitize(String value) {
    final buffer = StringBuffer();
    var hash = 0x811c9dc5;
    for (final unit in value.codeUnits) {
      hash ^= unit;
      hash = (hash * 0x01000193) & 0xffffffff;
      final allowed =
          (unit >= 48 && unit <= 57) ||
          (unit >= 65 && unit <= 90) ||
          (unit >= 97 && unit <= 122);
      buffer.write(allowed ? String.fromCharCode(unit) : '_');
    }
    return '${buffer}_h${hash.toRadixString(16).padLeft(8, '0')}';
  }

  void _ensureConnected() {
    if (!_connected || _closing || !_adapter.isConnected) {
      throw const MessagingConnectionException(
        'RabbitMQ adapter is not connected.',
      );
    }
  }

  void _ensureSchedulable(DateTime? runAt) {
    if (runAt == null || !runAt.isAfter(messagingConfig.now())) {
      return;
    }
    throw const MessagingUnsupportedException(
      'RabbitMQ delayed enqueue requires a delayed-message strategy.',
    );
  }
}

final class _RabbitMqSubscription implements Subscription {
  _RabbitMqSubscription({
    required this.consumer,
    required this.concurrency,
    required this.messagingConfig,
    required this.onClose,
  });

  final RabbitMqConsumer consumer;
  final int concurrency;
  final MessagingConfig messagingConfig;
  final void Function() onClose;
  final Queue<RabbitMqDelivery> _pending = Queue();
  final Set<Future<void>> _active = {};
  StreamSubscription<RabbitMqDelivery>? _subscription;
  var _closed = false;

  void listen(Future<void> Function(RabbitMqDelivery delivery) onDelivery) {
    _subscription = consumer.deliveries.listen(
      (delivery) {
        if (_closed) {
          unawaited(delivery.nack(requeue: true));
          return;
        }
        _pending.add(delivery);
        _drain(onDelivery);
      },
      onError: (Object error, StackTrace stackTrace) {
        messagingConfig.log(
          MessagingLogLevel.error,
          'RabbitMQ subscription stream failed.',
          error: error,
          stackTrace: stackTrace,
          attributes: {'transport': 'rabbitmq'},
        );
      },
    );
  }

  void _drain(Future<void> Function(RabbitMqDelivery delivery) onDelivery) {
    while (!_closed && _active.length < concurrency && _pending.isNotEmpty) {
      final delivery = _pending.removeFirst();
      late final Future<void> task;
      task = Future<void>.sync(() => onDelivery(delivery))
          .then<void>(
            (_) {},
            onError: (Object error, StackTrace stackTrace) {
              messagingConfig.log(
                MessagingLogLevel.error,
                'RabbitMQ subscription handler failed.',
                error: error,
                stackTrace: stackTrace,
                attributes: {'transport': 'rabbitmq'},
              );
            },
          )
          .whenComplete(() {
            _active.remove(task);
            _drain(onDelivery);
          });
      _active.add(task);
    }
  }

  @override
  Future<void> close() async {
    if (_closed) {
      return;
    }
    _closed = true;
    await _subscription?.cancel();
    while (_pending.isNotEmpty) {
      await _pending.removeFirst().nack(requeue: true);
    }
    if (_active.isNotEmpty) {
      await Future.wait(
        _active.toList(),
      ).timeout(messagingConfig.shutdownTimeout);
    }
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
  final Set<Future<void>> _tasks = {};
  StreamSubscription<RabbitMqDelivery>? _subscription;
  var _closed = false;

  void start() {
    _subscription = consumer.deliveries.listen(
      (delivery) {
        if (_closed) {
          unawaited(delivery.nack(requeue: true));
          return;
        }
        _pending.add(delivery);
        _drain();
      },
      onError: (Object error, StackTrace stackTrace) {
        bus._recordWorkerError(error);
        bus.messagingConfig.log(
          MessagingLogLevel.error,
          'RabbitMQ worker stream failed.',
          error: error,
          stackTrace: stackTrace,
          attributes: {'transport': 'rabbitmq', 'topic': topic},
        );
      },
    );
  }

  void _drain() {
    while (!_closed && _tasks.length < concurrency && _pending.isNotEmpty) {
      final delivery = _pending.removeFirst();
      late final Future<void> task;
      task = _process(delivery).whenComplete(() {
        _tasks.remove(task);
        _drain();
      });
      _tasks.add(task);
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
      bus.messagingConfig.recordMetric(
        'podbus.jobs.completed',
        attributes: {'transport': 'rabbitmq', 'topic': topic},
      );
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
      } on Object catch (failureError, failureStackTrace) {
        bus._recordWorkerError(failureError);
        bus.messagingConfig.log(
          MessagingLogLevel.error,
          'RabbitMQ worker failure handling failed.',
          error: failureError,
          stackTrace: failureStackTrace,
          attributes: {'transport': 'rabbitmq', 'topic': topic},
        );
      }
    }
  }

  @override
  Future<void> close() async {
    if (_closed) {
      return;
    }
    _closed = true;
    await _subscription?.cancel();
    while (_pending.isNotEmpty) {
      await _pending.removeFirst().nack(requeue: true);
    }
    if (_tasks.isNotEmpty) {
      await Future.wait(
        _tasks.toList(),
      ).timeout(bus.messagingConfig.shutdownTimeout);
    }
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
