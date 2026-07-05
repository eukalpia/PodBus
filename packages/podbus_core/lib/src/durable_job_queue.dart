import 'headers.dart';
import 'health.dart';
import 'message_context.dart';
import 'policies.dart';

typedef JobHandler<T> = Future<void> Function(JobContext context, T payload);

abstract interface class DurableJobQueue {
  Future<void> connect();

  Future<void> close({Duration? timeout});

  Future<void> enqueue<T>(
    String topic,
    T payload, {
    MessageHeaders? headers,
    String? idempotencyKey,
    DateTime? runAt,
    RetryPolicy? retryPolicy,
  });

  Future<Worker> worker<T>(
    String topic, {
    String? queueGroup,
    String? durableName,
    int concurrency = 1,
    RetryPolicy? retryPolicy,
    DeadLetterPolicy? deadLetterPolicy,
    required JobHandler<T> handler,
  });

  Future<HealthCheckResult> healthCheck();
}

abstract interface class Worker {
  Future<void> close();
}
