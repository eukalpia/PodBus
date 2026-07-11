import 'dart:convert';

import 'codec.dart';
import 'exceptions.dart';
import 'failure.dart';
import 'headers.dart';
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

final class MessagingLimits {
  const MessagingLimits({
    this.maxPayloadBytes = 1024 * 1024,
    this.maxHeaderCount = 64,
    this.maxHeaderBytes = 16 * 1024,
    this.maxErrorDetailCharacters = 4096,
  }) : assert(maxPayloadBytes > 0),
       assert(maxHeaderCount > 0),
       assert(maxHeaderBytes > 0),
       assert(maxErrorDetailCharacters > 0);

  final int maxPayloadBytes;
  final int maxHeaderCount;
  final int maxHeaderBytes;
  final int maxErrorDetailCharacters;

  void validatePayload(List<int> bytes) {
    if (bytes.length > maxPayloadBytes) {
      throw MessagingConfigurationException(
        'Encoded payload is ${bytes.length} bytes, exceeding the configured '
        'limit of $maxPayloadBytes bytes.',
      );
    }
  }

  void validateHeaders(Map<String, Object?> headers) {
    if (headers.length > maxHeaderCount) {
      throw MessagingConfigurationException(
        'Message has ${headers.length} headers, exceeding the configured '
        'limit of $maxHeaderCount.',
      );
    }

    var bytes = 0;
    for (final MapEntry(:key, :value) in headers.entries) {
      bytes += utf8.encode(key).length;
      if (value != null) {
        bytes += utf8.encode(value.toString()).length;
      }
    }
    if (bytes > maxHeaderBytes) {
      throw MessagingConfigurationException(
        'Message headers use $bytes bytes, exceeding the configured limit of '
        '$maxHeaderBytes bytes.',
      );
    }
  }

  String truncateError(Object value) {
    final text = value.toString();
    if (text.length <= maxErrorDetailCharacters) {
      return text;
    }
    return '${text.substring(0, maxErrorDetailCharacters)}…';
  }
}

final class MessagingConfig {
  MessagingConfig({
    MessageCodec? codec,
    MessageCodecRegistry? codecRegistry,
    RetryPolicy? defaultRetryPolicy,
    this.idempotencyStore,
    this.logHook,
    this.metricHook,
    this.requestTimeout = const Duration(seconds: 30),
    this.shutdownTimeout = const Duration(seconds: 10),
    this.limits = const MessagingLimits(),
    MessagingFailureClassifier? failureClassifier,
    DateTime Function()? clock,
  }) : codec = codec ?? JsonMessageCodec(registry: codecRegistry),
       defaultRetryPolicy =
           defaultRetryPolicy ??
           RetryPolicy(
             maxAttempts: 3,
             initialDelay: const Duration(milliseconds: 100),
             maxDelay: const Duration(seconds: 30),
             jitter: 0.2,
           ),
       failureClassifier =
           failureClassifier ?? defaultMessagingFailureClassifier,
       _clock = clock ?? DateTime.now {
    if (requestTimeout <= Duration.zero) {
      throw const MessagingConfigurationException(
        'Messaging request timeout must be greater than zero.',
      );
    }
    if (shutdownTimeout <= Duration.zero) {
      throw const MessagingConfigurationException(
        'Messaging shutdown timeout must be greater than zero.',
      );
    }
  }

  final MessageCodec codec;
  final RetryPolicy defaultRetryPolicy;
  final IdempotencyStore? idempotencyStore;
  final MessagingLogHook? logHook;
  final MessagingMetricHook? metricHook;
  final Duration requestTimeout;
  final Duration shutdownTimeout;
  final MessagingLimits limits;
  final MessagingFailureClassifier failureClassifier;
  final DateTime Function() _clock;

  DateTime now() => _clock();

  bool shouldRetry(Object error) {
    return isRetryableMessagingFailure(failureClassifier(error));
  }

  void validateOutbound(EncodedMessage encoded, MessageHeaders headers) {
    limits.validatePayload(encoded.bytes);
    limits.validateHeaders(headers.toMap());
  }

  void validateRawOutbound(List<int> bytes, Map<String, Object?> headers) {
    limits.validatePayload(bytes);
    limits.validateHeaders(headers);
  }

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

  void recordDuration(
    String name,
    Duration duration, {
    Map<String, Object?> attributes = const {},
  }) {
    recordMetric(
      name,
      value: duration.inMicroseconds,
      attributes: {...attributes, 'unit': 'microseconds'},
    );
  }
}
