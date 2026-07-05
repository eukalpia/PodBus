import 'package:podbus_core/podbus_core.dart';

import 'config.dart';

final class KafkaEventBus implements MessageBus, DurableJobQueue {
  KafkaEventBus({required this.config});

  final KafkaMessagingConfig config;
  var _connected = false;

  @override
  Future<void> connect() async {
    _connected = true;
  }

  @override
  Future<void> close({Duration? timeout}) async {
    _connected = false;
  }

  @override
  Future<void> publish<T>(
    String subject,
    T payload, {
    MessageHeaders? headers,
  }) {
    throw const MessagingUnsupportedException(
      'Kafka adapter is experimental and producer support is not implemented yet.',
    );
  }

  @override
  Future<Subscription> subscribe<T>(
    String subject, {
    String? queueGroup,
    required MessageHandler<T> handler,
  }) {
    throw const MessagingUnsupportedException(
      'Kafka adapter is experimental and consumer group support is not implemented yet.',
    );
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
  }) {
    throw const MessagingUnsupportedException(
      'Kafka durable job mapping is experimental and not implemented yet.',
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
  }) {
    throw const MessagingUnsupportedException(
      'Kafka worker mapping is experimental and not implemented yet.',
    );
  }

  @override
  Future<HealthCheckResult> healthCheck() async {
    if (!_connected) {
      return HealthCheckResult.unhealthy(
        message: 'Kafka adapter is not connected.',
      );
    }
    return HealthCheckResult(
      status: HealthStatus.degraded,
      checkedAt: DateTime.now(),
      message:
          'Kafka adapter scaffold is connected but transport operations are not implemented.',
    );
  }
}
