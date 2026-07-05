import 'headers.dart';
import 'health.dart';
import 'message_context.dart';

typedef MessageHandler<T> =
    Future<void> Function(MessageContext context, T payload);

abstract interface class MessageBus {
  Future<void> connect();

  Future<void> close({Duration? timeout});

  Future<void> publish<T>(String subject, T payload, {MessageHeaders? headers});

  Future<Subscription> subscribe<T>(
    String subject, {
    String? queueGroup,
    required MessageHandler<T> handler,
  });

  Future<TResponse> request<TRequest, TResponse>(
    String subject,
    TRequest payload, {
    MessageHeaders? headers,
    Duration? timeout,
  });

  Future<HealthCheckResult> healthCheck();
}

abstract interface class Subscription {
  Future<void> close();
}
