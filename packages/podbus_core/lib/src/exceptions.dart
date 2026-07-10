base class MessagingException implements Exception {
  const MessagingException(this.message, {this.cause, this.stackTrace});

  final String message;
  final Object? cause;
  final StackTrace? stackTrace;

  @override
  String toString() => '$runtimeType: $message';
}

final class MessagingConfigurationException extends MessagingException {
  const MessagingConfigurationException(
    super.message, {
    super.cause,
    super.stackTrace,
  });
}

final class MessageCodecException extends MessagingException {
  const MessageCodecException(super.message, {super.cause, super.stackTrace});
}

final class MessagingConnectionException extends MessagingException {
  const MessagingConnectionException(
    super.message, {
    super.cause,
    super.stackTrace,
  });
}

final class MessagingTimeoutException extends MessagingException {
  const MessagingTimeoutException(
    super.message, {
    this.timeout,
    super.cause,
    super.stackTrace,
  });

  final Duration? timeout;
}

final class MessagingUnsupportedException extends MessagingException {
  const MessagingUnsupportedException(
    super.message, {
    super.cause,
    super.stackTrace,
  });
}

final class MessagingAuthenticationException extends MessagingException {
  const MessagingAuthenticationException(
    super.message, {
    super.cause,
    super.stackTrace,
  });
}

final class MessagingRateLimitException extends MessagingException {
  const MessagingRateLimitException(
    super.message, {
    super.cause,
    super.stackTrace,
  });
}
