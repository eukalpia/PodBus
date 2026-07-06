// ignore_for_file: prefer_initializing_formals

import 'dart:async';
import 'dart:convert';

import 'package:podbus_core/podbus_core.dart';

import 'config.dart';
import 'kafka_adapter.dart';

final class KafkaEventBus implements MessageBus, DurableJobQueue {
  KafkaEventBus({
    required this.config,
    KafkaAdapter? adapter,
    MessageCodec? codec,
  }) : _adapter = adapter ?? DartKafkaAdapter(),
       _codec = codec ?? const JsonMessageCodec();

  static const _deadLetterSourceHeader = 'podbus-dead-letter-source';
  static const _deadLetterErrorHeader = 'podbus-dead-letter-error';
  static const _deadLetterStackTraceHeader = 'podbus-dead-letter-stack-trace';

  final KafkaMessagingConfig config;
  final KafkaAdapter _adapter;
  final MessageCodec _codec;
  final List<_KafkaSubscription> _subscriptions = [];
  final List<_KafkaWorker> _workers = [];
  var _connected = false;

  @override
  Future<void> connect() async {
    if (!config.experimental) {
      throw const MessagingConfigurationException(
        'Kafka adapter must be explicitly marked experimental.',
      );
    }
    await _adapter.connect(config);
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
    await _produceEnvelope(
      topic: subject,
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
    final consumer = await _adapter.consumerFor(
      topics: [subject],
      groupId: queueGroup ?? config.groupId,
    );
    late final _KafkaSubscription subscription;
    subscription = _KafkaSubscription(
      subject: subject,
      consumer: consumer,
      pollTimeout: _pollTimeout,
      onClose: () => _subscriptions.remove(subscription),
      process: (record) async {
        await _handleEventRecord(record, consumer, handler);
      },
    );
    _subscriptions.add(subscription);
    subscription.start();
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
      'Kafka is an event log and does not expose generic request/reply semantics.',
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
    _ensureKafkaRetryPolicy(retryPolicy);
    final effectiveHeaders = (headers ?? MessageHeaders()).copyWith(
      idempotencyKey: idempotencyKey,
    );
    await _produceEnvelope(
      topic: topic,
      payload: payload,
      headers: effectiveHeaders,
      key: idempotencyKey ?? effectiveHeaders.idempotencyKey,
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
    if (concurrency != 1) {
      throw const MessagingUnsupportedException(
        'Kafka workers process one partition stream at a time in this adapter.',
      );
    }
    _ensureKafkaRetryPolicy(retryPolicy);

    final consumer = await _adapter.consumerFor(
      topics: [topic],
      groupId: durableName ?? queueGroup ?? config.groupId,
    );
    late final _KafkaWorker worker;
    worker = _KafkaWorker(
      topic: topic,
      consumer: consumer,
      pollTimeout: _pollTimeout,
      onClose: () => _workers.remove(worker),
      process: (record) async {
        await _handleJobRecord(
          topic,
          record,
          consumer,
          deadLetterPolicy ?? const DeadLetterPolicy.disabled(),
          handler,
        );
      },
    );
    _workers.add(worker);
    worker.start();
    return worker;
  }

  @override
  Future<HealthCheckResult> healthCheck() async {
    if (!_connected || !_adapter.isConnected) {
      return HealthCheckResult.unhealthy(
        message: 'Kafka adapter is not connected.',
      );
    }
    for (final subscription in _subscriptions) {
      final lastError = subscription.lastError;
      if (lastError != null) {
        return HealthCheckResult.unhealthy(
          message: 'Kafka consumer loop failed.',
          details: {
            'consumerType': 'subscription',
            'subject': subscription.subject,
            'lastConsumerError': lastError.toString(),
            if (subscription.lastStackTrace != null)
              'lastConsumerStackTrace': subscription.lastStackTrace.toString(),
          },
        );
      }
    }
    for (final worker in _workers) {
      final lastError = worker.lastError;
      if (lastError != null) {
        return HealthCheckResult.unhealthy(
          message: 'Kafka consumer loop failed.',
          details: {
            'consumerType': 'worker',
            'topic': worker.topic,
            'lastConsumerError': lastError.toString(),
            if (worker.lastStackTrace != null)
              'lastConsumerStackTrace': worker.lastStackTrace.toString(),
          },
        );
      }
    }
    try {
      await _adapter.flush(config.requestTimeout);
      return HealthCheckResult.healthy(
        message: 'Kafka adapter is connected.',
        details: {
          'experimental': true,
          'subscriptions': _subscriptions.length,
          'workers': _workers.length,
        },
      );
    } on Object catch (error, stackTrace) {
      return HealthCheckResult(
        status: HealthStatus.unhealthy,
        checkedAt: DateTime.now(),
        message: 'Kafka health check failed.',
        details: {
          'error': error.toString(),
          'stackTrace': stackTrace.toString(),
        },
      );
    }
  }

  Future<void> _handleEventRecord<T>(
    KafkaAdapterRecord record,
    KafkaAdapterConsumer consumer,
    MessageHandler<T> handler,
  ) async {
    final decoded = await _decodeRecord<T>(record);
    final context = _KafkaMessageContext(
      subject: record.topic,
      headers: decoded.headers,
      rawMessage: record,
      consumer: consumer,
    );
    await handler(context, decoded.payload);
    if (!context.completed) {
      await context.ack();
    }
  }

  Future<void> _handleJobRecord<T>(
    String topic,
    KafkaAdapterRecord record,
    KafkaAdapterConsumer consumer,
    DeadLetterPolicy deadLetterPolicy,
    JobHandler<T> handler,
  ) async {
    final decoded = await _decodeRecord<T>(record);
    final context = _KafkaJobContext(
      topic: topic,
      headers: decoded.headers,
      rawMessage: record,
      attempt: decoded.headers.attempt,
      maxAttempts: 1,
      consumer: consumer,
      bus: this,
      record: record,
      deadLetterPolicy: deadLetterPolicy,
    );
    try {
      await handler(context, decoded.payload);
      if (!context.completed) {
        await context.ack();
      }
    } on Object catch (error, stackTrace) {
      if (!deadLetterPolicy.enabled) {
        Error.throwWithStackTrace(error, stackTrace);
      }
      await _publishDeadLetter(
        topic,
        record,
        decoded.headers,
        deadLetterPolicy,
        error: error,
        stackTrace: stackTrace,
      );
      await consumer.commit();
    }
  }

  Future<void> _produceEnvelope<T>({
    required String topic,
    required T payload,
    required MessageHeaders headers,
    String? key,
  }) async {
    final encoded = await _codec.encode(payload);
    await _adapter.produce(
      topic: topic,
      key: key,
      bytes: _envelopeBytes(
        headers: headers,
        contentType: encoded.contentType,
        schemaVersion: encoded.schemaVersion,
        payload: encoded.bytes,
      ),
    );
  }

  Future<void> _publishDeadLetter(
    String sourceTopic,
    KafkaAdapterRecord record,
    MessageHeaders headers,
    DeadLetterPolicy policy, {
    Object? error,
    StackTrace? stackTrace,
  }) async {
    final destination = policy.destination ?? '$sourceTopic.dead-letter';
    final deadLetterHeaders = headers.copyWith(
      custom: {
        ...headers.custom,
        _deadLetterSourceHeader: sourceTopic,
        if (policy.includeErrorDetails && error != null)
          _deadLetterErrorHeader: error.toString(),
        if (policy.includeErrorDetails && stackTrace != null)
          _deadLetterStackTraceHeader: stackTrace.toString(),
      },
    );
    final decoded = _decodeEnvelope(record.bytes);
    await _adapter.produce(
      topic: destination,
      key: record.key,
      bytes: _envelopeBytes(
        headers: deadLetterHeaders,
        contentType: decoded.contentType,
        schemaVersion: decoded.schemaVersion,
        payload: decoded.payloadBytes,
      ),
    );
  }

  Future<_DecodedKafkaPayload<T>> _decodeRecord<T>(
    KafkaAdapterRecord record,
  ) async {
    final envelope = _decodeEnvelope(record.bytes);
    final payload = await _codec.decode<T>(
      EncodedMessage(
        bytes: envelope.payloadBytes,
        contentType: envelope.contentType,
        schemaVersion: envelope.schemaVersion,
      ),
    );
    return _DecodedKafkaPayload(payload: payload, headers: envelope.headers);
  }

  _KafkaEnvelope _decodeEnvelope(List<int> bytes) {
    try {
      final json = jsonDecode(utf8.decode(bytes)) as Map<String, Object?>;
      final headerMap = json['headers'] as Map<String, Object?>? ?? const {};
      return _KafkaEnvelope(
        headers: MessageHeaders.fromMap(headerMap),
        contentType:
            json['contentType'] as String? ?? JsonMessageCodec.contentType,
        schemaVersion: switch (json['schemaVersion']) {
          final int value => value,
          final String value => int.parse(value),
          null => 1,
          final Object value => throw MessageCodecException(
            'Kafka envelope schemaVersion must be an int or string, got '
            '${value.runtimeType}.',
          ),
        },
        payloadBytes: base64Decode(json['payload'] as String),
      );
    } on MessagingException {
      rethrow;
    } on Object catch (error, stackTrace) {
      throw MessageCodecException(
        'Failed to decode Kafka PodBus envelope.',
        cause: error,
        stackTrace: stackTrace,
      );
    }
  }

  List<int> _envelopeBytes({
    required MessageHeaders headers,
    required String contentType,
    required int schemaVersion,
    required List<int> payload,
  }) {
    return utf8.encode(
      jsonEncode({
        'headers': headers.toMap(),
        'contentType': contentType,
        'schemaVersion': schemaVersion,
        'payload': base64Encode(payload),
      }),
    );
  }

  void _ensureConnected() {
    if (!_connected) {
      throw const MessagingConnectionException(
        'Kafka adapter is not connected.',
      );
    }
  }

  void _ensureSchedulable(DateTime? runAt) {
    if (runAt == null || !runAt.isAfter(DateTime.now())) {
      return;
    }
    throw const MessagingUnsupportedException(
      'Kafka does not support scheduled enqueue in this adapter.',
    );
  }

  void _ensureKafkaRetryPolicy(RetryPolicy? retryPolicy) {
    if (retryPolicy == null || retryPolicy.maxAttempts == 1) {
      return;
    }
    throw const MessagingUnsupportedException(
      'Kafka retry requires an explicit retry topic strategy; automatic ack/nack retry is not implemented.',
    );
  }

  Duration get _pollTimeout {
    const maxPollTimeout = Duration(seconds: 1);
    return config.requestTimeout > maxPollTimeout
        ? maxPollTimeout
        : config.requestTimeout;
  }
}

final class _KafkaSubscription implements Subscription {
  _KafkaSubscription({
    required this.subject,
    required this.consumer,
    required this.pollTimeout,
    required this.process,
    required this.onClose,
  });

  final String subject;
  final KafkaAdapterConsumer consumer;
  final Duration pollTimeout;
  final Future<void> Function(KafkaAdapterRecord record) process;
  final void Function() onClose;
  Future<void>? _task;
  Object? lastError;
  StackTrace? lastStackTrace;
  var _closed = false;

  void start() {
    _task = _run();
  }

  Future<void> _run() async {
    while (!_closed) {
      try {
        final record = await consumer.poll(pollTimeout);
        if (record == null) {
          continue;
        }
        await process(record);
      } on Object catch (error, stackTrace) {
        lastError = error;
        lastStackTrace = stackTrace;
        break;
      }
    }
  }

  @override
  Future<void> close() async {
    if (_closed) {
      return;
    }
    _closed = true;
    await _task;
    await consumer.close();
    onClose();
  }
}

final class _KafkaWorker implements Worker {
  _KafkaWorker({
    required this.topic,
    required this.consumer,
    required this.pollTimeout,
    required this.process,
    required this.onClose,
  });

  final String topic;
  final KafkaAdapterConsumer consumer;
  final Duration pollTimeout;
  final Future<void> Function(KafkaAdapterRecord record) process;
  final void Function() onClose;
  Future<void>? _task;
  Object? lastError;
  StackTrace? lastStackTrace;
  var _closed = false;

  void start() {
    _task = _run();
  }

  Future<void> _run() async {
    while (!_closed) {
      try {
        final record = await consumer.poll(pollTimeout);
        if (record == null) {
          continue;
        }
        await process(record);
      } on Object catch (error, stackTrace) {
        lastError = error;
        lastStackTrace = stackTrace;
        break;
      }
    }
  }

  @override
  Future<void> close() async {
    if (_closed) {
      return;
    }
    _closed = true;
    await _task;
    await consumer.close();
    onClose();
  }
}

final class _KafkaMessageContext implements MessageContext {
  _KafkaMessageContext({
    required this.subject,
    required this.headers,
    required this.rawMessage,
    required this.consumer,
  });

  final KafkaAdapterConsumer consumer;
  var completed = false;

  @override
  final String subject;

  @override
  final MessageHeaders headers;

  @override
  final Object? rawMessage;

  @override
  Future<void> ack() async {
    await consumer.commit();
    completed = true;
  }

  @override
  Future<void> extendVisibility(Duration duration) {
    throw const MessagingUnsupportedException(
      'Kafka does not support visibility extension.',
    );
  }

  @override
  Future<void> nak({Duration? delay}) {
    throw const MessagingUnsupportedException(
      'Kafka does not support negative acknowledgements.',
    );
  }

  @override
  Future<void> reply<T>(T payload, {MessageHeaders? headers}) {
    throw const MessagingUnsupportedException(
      'Kafka is an event log and does not expose generic request/reply semantics.',
    );
  }

  @override
  Future<void> terminate() {
    throw const MessagingUnsupportedException(
      'Kafka does not support message termination.',
    );
  }
}

final class _KafkaJobContext implements JobContext {
  _KafkaJobContext({
    required this.topic,
    required this.headers,
    required this.rawMessage,
    required this.attempt,
    required this.maxAttempts,
    required this.consumer,
    required this.bus,
    required this.record,
    required this.deadLetterPolicy,
  });

  final KafkaAdapterConsumer consumer;
  final KafkaEventBus bus;
  final KafkaAdapterRecord record;
  final DeadLetterPolicy deadLetterPolicy;
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
    await consumer.commit();
    completed = true;
  }

  @override
  Future<void> deadLetter({Object? error, StackTrace? stackTrace}) async {
    await bus._publishDeadLetter(
      topic,
      record,
      headers,
      deadLetterPolicy,
      error: error,
      stackTrace: stackTrace,
    );
    await consumer.commit();
    completed = true;
  }

  @override
  Future<void> fail(Object error, [StackTrace? stackTrace]) async {
    Error.throwWithStackTrace(error, stackTrace ?? StackTrace.current);
  }

  @override
  Future<void> retry({Duration? delay}) {
    throw const MessagingUnsupportedException(
      'Kafka retry requires an explicit retry topic strategy.',
    );
  }
}

final class _KafkaEnvelope {
  const _KafkaEnvelope({
    required this.headers,
    required this.contentType,
    required this.schemaVersion,
    required this.payloadBytes,
  });

  final MessageHeaders headers;
  final String contentType;
  final int schemaVersion;
  final List<int> payloadBytes;
}

final class _DecodedKafkaPayload<T> {
  const _DecodedKafkaPayload({required this.payload, required this.headers});

  final T payload;
  final MessageHeaders headers;
}
