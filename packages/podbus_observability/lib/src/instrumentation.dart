import 'package:podbus_core/podbus_core.dart';

import 'trace_context.dart';

enum PodBusSpanKind { producer, consumer, client, internal }

enum PodBusSpanStatus { ok, error }

typedef PodBusSpanExporter = void Function(PodBusSpanRecord span);

final class PodBusSpanRecord {
  const PodBusSpanRecord({
    required this.name,
    required this.kind,
    required this.status,
    required this.traceId,
    required this.spanId,
    required this.parentSpanId,
    required this.startedAt,
    required this.endedAt,
    required this.attributes,
    this.errorType,
    this.errorMessage,
  });

  final String name;
  final PodBusSpanKind kind;
  final PodBusSpanStatus status;
  final String traceId;
  final String spanId;
  final String? parentSpanId;
  final DateTime startedAt;
  final DateTime endedAt;
  final Map<String, Object?> attributes;
  final String? errorType;
  final String? errorMessage;

  Duration get duration => endedAt.difference(startedAt);
}

final class PodBusTracer {
  PodBusTracer({
    required this.export,
    DateTime Function()? clock,
    this.maxErrorCharacters = 1024,
  }) : _clock = clock ?? DateTime.now {
    if (maxErrorCharacters < 1) {
      throw const MessagingConfigurationException(
        'Trace maxErrorCharacters must be greater than zero.',
      );
    }
  }

  final PodBusSpanExporter export;
  final DateTime Function() _clock;
  final int maxErrorCharacters;

  PodBusActiveSpan start(
    String name, {
    required PodBusSpanKind kind,
    W3cTraceContext? parent,
    Map<String, Object?> attributes = const {},
  }) {
    final context = parent?.child() ?? W3cTraceContext.root();
    return PodBusActiveSpan._(
      tracer: this,
      name: name,
      kind: kind,
      context: context,
      parentSpanId: parent?.spanId,
      startedAt: _clock(),
      attributes: Map.unmodifiable(attributes),
    );
  }

  String _truncate(Object value) {
    final text = value.toString();
    if (text.length <= maxErrorCharacters) {
      return text;
    }
    return '${text.substring(0, maxErrorCharacters)}…';
  }
}

final class PodBusActiveSpan {
  PodBusActiveSpan._({
    required this.tracer,
    required this.name,
    required this.kind,
    required this.context,
    required this.parentSpanId,
    required this.startedAt,
    required this.attributes,
  });

  final PodBusTracer tracer;
  final String name;
  final PodBusSpanKind kind;
  final W3cTraceContext context;
  final String? parentSpanId;
  final DateTime startedAt;
  final Map<String, Object?> attributes;
  var _ended = false;

  void end({Object? error}) {
    if (_ended) {
      return;
    }
    _ended = true;
    tracer.export(
      PodBusSpanRecord(
        name: name,
        kind: kind,
        status: error == null ? PodBusSpanStatus.ok : PodBusSpanStatus.error,
        traceId: context.traceId,
        spanId: context.spanId,
        parentSpanId: parentSpanId,
        startedAt: startedAt,
        endedAt: tracer._clock(),
        attributes: attributes,
        errorType: error?.runtimeType.toString(),
        errorMessage: error == null ? null : tracer._truncate(error),
      ),
    );
  }
}

final class InstrumentedMessageBus implements MessageBus {
  const InstrumentedMessageBus({
    required this.delegate,
    required this.tracer,
    this.transport = 'unknown',
  });

  final MessageBus delegate;
  final PodBusTracer tracer;
  final String transport;

  @override
  MessagingCapabilities get capabilities => delegate.capabilities;

  @override
  Future<void> connect() => delegate.connect();

  @override
  Future<void> close({Duration? timeout}) => delegate.close(timeout: timeout);

  @override
  Future<HealthCheckResult> healthCheck() => delegate.healthCheck();

  @override
  Future<void> publish<T>(
    String subject,
    T payload, {
    MessageHeaders? headers,
  }) async {
    final sourceHeaders = headers ?? MessageHeaders();
    final parent = W3cTraceContext.extract(sourceHeaders);
    final span = tracer.start(
      'podbus publish $subject',
      kind: PodBusSpanKind.producer,
      parent: parent,
      attributes: {'transport': transport, 'subject': subject},
    );
    try {
      await delegate.publish(
        subject,
        payload,
        headers: span.context.inject(sourceHeaders),
      );
      span.end();
    } on Object catch (error, stackTrace) {
      span.end(error: error);
      Error.throwWithStackTrace(error, stackTrace);
    }
  }

  @override
  Future<TResponse> request<TRequest, TResponse>(
    String subject,
    TRequest payload, {
    MessageHeaders? headers,
    Duration? timeout,
  }) async {
    final sourceHeaders = headers ?? MessageHeaders();
    final parent = W3cTraceContext.extract(sourceHeaders);
    final span = tracer.start(
      'podbus request $subject',
      kind: PodBusSpanKind.client,
      parent: parent,
      attributes: {'transport': transport, 'subject': subject},
    );
    try {
      final response = await delegate.request<TRequest, TResponse>(
        subject,
        payload,
        headers: span.context.inject(sourceHeaders),
        timeout: timeout,
      );
      span.end();
      return response;
    } on Object catch (error, stackTrace) {
      span.end(error: error);
      Error.throwWithStackTrace(error, stackTrace);
    }
  }

  @override
  Future<Subscription> subscribe<T>(
    String subject, {
    String? queueGroup,
    int concurrency = 1,
    required MessageHandler<T> handler,
  }) {
    return delegate.subscribe<T>(
      subject,
      queueGroup: queueGroup,
      concurrency: concurrency,
      handler: (context, payload) async {
        final parent = W3cTraceContext.extract(context.headers);
        final span = tracer.start(
          'podbus consume $subject',
          kind: PodBusSpanKind.consumer,
          parent: parent,
          attributes: {
            'transport': transport,
            'subject': subject,
            if (queueGroup != null) 'queueGroup': queueGroup,
          },
        );
        try {
          await handler(context, payload);
          span.end();
        } on Object catch (error, stackTrace) {
          span.end(error: error);
          Error.throwWithStackTrace(error, stackTrace);
        }
      },
    );
  }
}

final class InstrumentedDurableJobQueue implements DurableJobQueue {
  const InstrumentedDurableJobQueue({
    required this.delegate,
    required this.tracer,
    this.transport = 'unknown',
  });

  final DurableJobQueue delegate;
  final PodBusTracer tracer;
  final String transport;

  @override
  MessagingCapabilities get capabilities => delegate.capabilities;

  @override
  Future<void> connect() => delegate.connect();

  @override
  Future<void> close({Duration? timeout}) => delegate.close(timeout: timeout);

  @override
  Future<HealthCheckResult> healthCheck() => delegate.healthCheck();

  @override
  Future<void> enqueue<T>(
    String topic,
    T payload, {
    MessageHeaders? headers,
    String? idempotencyKey,
    DateTime? runAt,
    RetryPolicy? retryPolicy,
  }) async {
    final sourceHeaders = headers ?? MessageHeaders();
    final parent = W3cTraceContext.extract(sourceHeaders);
    final span = tracer.start(
      'podbus enqueue $topic',
      kind: PodBusSpanKind.producer,
      parent: parent,
      attributes: {'transport': transport, 'topic': topic},
    );
    try {
      await delegate.enqueue(
        topic,
        payload,
        headers: span.context.inject(sourceHeaders),
        idempotencyKey: idempotencyKey,
        runAt: runAt,
        retryPolicy: retryPolicy,
      );
      span.end();
    } on Object catch (error, stackTrace) {
      span.end(error: error);
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
  }) {
    return delegate.worker<T>(
      topic,
      queueGroup: queueGroup,
      durableName: durableName,
      concurrency: concurrency,
      retryPolicy: retryPolicy,
      deadLetterPolicy: deadLetterPolicy,
      handler: (context, payload) async {
        final parent = W3cTraceContext.extract(context.headers);
        final span = tracer.start(
          'podbus process $topic',
          kind: PodBusSpanKind.consumer,
          parent: parent,
          attributes: {
            'transport': transport,
            'topic': topic,
            'attempt': context.attempt,
            'maxAttempts': context.maxAttempts,
            if (queueGroup != null) 'queueGroup': queueGroup,
            if (durableName != null) 'durableName': durableName,
          },
        );
        try {
          await handler(context, payload);
          span.end();
        } on Object catch (error, stackTrace) {
          span.end(error: error);
          Error.throwWithStackTrace(error, stackTrace);
        }
      },
    );
  }
}
