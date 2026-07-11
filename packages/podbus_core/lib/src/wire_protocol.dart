abstract final class PodBusWireHeaders {
  static const contentType = 'podbus-content-type';
  static const schemaVersion = 'podbus-schema-version';
  static const messageType = 'podbus-message-type';

  static const deadLetterSource = 'podbus-dead-letter-source';
  static const deadLetterError = 'podbus-dead-letter-error';
  static const deadLetterStackTrace = 'podbus-dead-letter-stack-trace';
  static const deadLetterPayloadOmitted = 'podbus-dead-letter-payload-omitted';

  static const retryMaxAttempts = 'podbus-retry-max-attempts';
  static const retryInitialDelayMicros = 'podbus-retry-initial-delay-micros';
  static const retryMaxDelayMicros = 'podbus-retry-max-delay-micros';
  static const retryBackoffMultiplier = 'podbus-retry-backoff-multiplier';
  static const retryJitter = 'podbus-retry-jitter';
}
