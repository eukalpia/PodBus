import 'codec.dart';
import 'idempotency_store.dart';
import 'policies.dart';

typedef MessagingLogHook = void Function(MessagingLogEvent event);
typedef MessagingMetricHook = void Function(MessagingMetricEvent event);

enum MessagingLogLevel { debug, info, warning, error }

final class MessagingLogEvent {
  const MessagingLogEvent({
    required this.level,
    required this.message,
    required this.timestamp,
    this.error,
    this.stackTrace,
    this.attributes = const {},
  });

  final MessagingLogLevel level;
  final String message;
  final DateTime timestamp;
  final Object? error;
  final StackTrace? stackTrace;
  final Map<String, Object?> attributes;
}

final class MessagingMetricEvent {
  const MessagingMetricEvent({
    required this.name,
    required this.value,
    required this.timestamp,
    this.attributes = const {},
  });

  final String name;
  final num value;
  final DateTime timestamp;
  final Map<String, Object?> attributes;
}

final class MessagingConfig {
  MessagingConfig({
    MessageCodec? codec,
    RetryPolicy? defaultRetryPolicy,
    this.idempotencyStore,
    this.logHook,
    this.metricHook,
    this.requestTimeout = const Duration(seconds: 30),
    DateTime Function()? clock,
  }) : codec = codec ?? const JsonMessageCodec(),
       defaultRetryPolicy =
           defaultRetryPolicy ??
           RetryPolicy(
             maxAttempts: 3,
             initialDelay: const Duration(milliseconds: 100),
             maxDelay: const Duration(seconds: 30),
           ),
       _clock = clock ?? DateTime.now;

  final MessageCodec codec;
  final RetryPolicy defaultRetryPolicy;
  final IdempotencyStore? idempotencyStore;
  final MessagingLogHook? logHook;
  final MessagingMetricHook? metricHook;
  final Duration requestTimeout;
  final DateTime Function() _clock;

  void log(
    MessagingLogLevel level,
    String message, {
    Object? error,
    StackTrace? stackTrace,
    Map<String, Object?> attributes = const {},
  }) {
    logHook?.call(
      MessagingLogEvent(
        level: level,
        message: message,
        timestamp: _clock(),
        error: error,
        stackTrace: stackTrace,
        attributes: attributes,
      ),
    );
  }

  void recordMetric(
    String name, {
    num value = 1,
    Map<String, Object?> attributes = const {},
  }) {
    metricHook?.call(
      MessagingMetricEvent(
        name: name,
        value: value,
        timestamp: _clock(),
        attributes: attributes,
      ),
    );
  }
}
