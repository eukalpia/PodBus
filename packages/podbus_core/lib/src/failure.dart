import 'exceptions.dart';

enum MessagingFailureKind {
  transient,
  permanent,
  malformed,
  unauthorized,
  rateLimited,
  infrastructure,
}

typedef MessagingFailureClassifier =
    MessagingFailureKind Function(Object error);

MessagingFailureKind defaultMessagingFailureClassifier(Object error) {
  return switch (error) {
    MessageCodecException() => MessagingFailureKind.malformed,
    MessagingConfigurationException() => MessagingFailureKind.permanent,
    MessagingUnsupportedException() => MessagingFailureKind.permanent,
    MessagingAuthenticationException() => MessagingFailureKind.unauthorized,
    MessagingRateLimitException() => MessagingFailureKind.rateLimited,
    MessagingConnectionException() => MessagingFailureKind.infrastructure,
    MessagingTimeoutException() => MessagingFailureKind.transient,
    _ => MessagingFailureKind.transient,
  };
}

bool isRetryableMessagingFailure(MessagingFailureKind kind) {
  return switch (kind) {
    MessagingFailureKind.transient ||
    MessagingFailureKind.rateLimited ||
    MessagingFailureKind.infrastructure => true,
    MessagingFailureKind.permanent ||
    MessagingFailureKind.malformed ||
    MessagingFailureKind.unauthorized => false,
  };
}
