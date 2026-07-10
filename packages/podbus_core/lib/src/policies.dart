import 'dart:math' as math;

import 'exceptions.dart';

final class RetryPolicy {
  RetryPolicy({
    required this.maxAttempts,
    required this.initialDelay,
    required this.maxDelay,
    this.backoffMultiplier = 2,
    this.jitter = 0,
  }) {
    if (maxAttempts < 1) {
      throw const MessagingConfigurationException(
        'Retry maxAttempts must be greater than zero.',
      );
    }
    if (initialDelay.isNegative) {
      throw const MessagingConfigurationException(
        'Retry initialDelay cannot be negative.',
      );
    }
    if (maxDelay < initialDelay) {
      throw const MessagingConfigurationException(
        'Retry maxDelay must be greater than or equal to initialDelay.',
      );
    }
    if (backoffMultiplier < 1) {
      throw const MessagingConfigurationException(
        'Retry backoffMultiplier must be at least 1.',
      );
    }
    if (jitter < 0 || jitter > 1) {
      throw const MessagingConfigurationException(
        'Retry jitter must be between 0 and 1.',
      );
    }
  }

  static final math.Random _random = math.Random();

  final int maxAttempts;
  final Duration initialDelay;
  final Duration maxDelay;
  final double backoffMultiplier;
  final double jitter;

  Duration delayForAttempt(int attempt, {double? randomValue}) {
    if (attempt < 1) {
      throw const MessagingConfigurationException(
        'Retry attempt must be greater than zero.',
      );
    }
    if (randomValue != null && (randomValue < 0 || randomValue > 1)) {
      throw const MessagingConfigurationException(
        'Retry randomValue must be between 0 and 1.',
      );
    }

    final multiplier = math.pow(backoffMultiplier, attempt - 1).toDouble();
    final uncappedMicros = initialDelay.inMicroseconds * multiplier;
    final cappedMicros = math.min(
      uncappedMicros,
      maxDelay.inMicroseconds.toDouble(),
    );
    if (jitter == 0 || cappedMicros == 0) {
      return Duration(microseconds: cappedMicros.round());
    }

    final sample = randomValue ?? _random.nextDouble();
    final jitterFactor = 1 - jitter + (2 * jitter * sample);
    final jitteredMicros = math.min(
      cappedMicros * jitterFactor,
      maxDelay.inMicroseconds.toDouble(),
    );
    return Duration(microseconds: math.max(0, jitteredMicros.round()));
  }
}

final class DeadLetterPolicy {
  const DeadLetterPolicy({
    required this.enabled,
    this.destination,
    this.includeErrorDetails = false,
    this.includeOriginalPayload = false,
  });

  const DeadLetterPolicy.disabled()
    : enabled = false,
      destination = null,
      includeErrorDetails = false,
      includeOriginalPayload = false;

  final bool enabled;
  final String? destination;
  final bool includeErrorDetails;
  final bool includeOriginalPayload;
}
