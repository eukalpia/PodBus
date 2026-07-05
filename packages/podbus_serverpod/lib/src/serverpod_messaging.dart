import 'package:podbus_core/podbus_core.dart';

typedef ServerpodSessionFactory<TSession> = Future<TSession> Function();

typedef ServerpodMessageHandler<TSession, TPayload> =
    Future<void> Function(
      TSession session,
      MessageContext context,
      TPayload payload,
    );

typedef ServerpodJobHandler<TSession, TPayload> =
    Future<void> Function(
      TSession session,
      JobContext context,
      TPayload payload,
    );

final class ServerpodMessaging<TSession> {
  ServerpodMessaging({
    required this.bus,
    required this.queue,
    required this.sessionFactory,
  });

  final MessageBus bus;
  final DurableJobQueue queue;
  final ServerpodSessionFactory<TSession> sessionFactory;

  Future<void> start() async {
    await bus.connect();
    await queue.connect();
  }

  Future<void> stop({Duration? timeout}) async {
    await queue.close(timeout: timeout);
    await bus.close(timeout: timeout);
  }

  Future<Subscription> subscribe<TPayload>(
    String subject, {
    String? queueGroup,
    required ServerpodMessageHandler<TSession, TPayload> handler,
  }) {
    return bus.subscribe<TPayload>(
      subject,
      queueGroup: queueGroup,
      handler: (context, payload) async {
        final session = await sessionFactory();
        await handler(session, context, payload);
      },
    );
  }

  Future<Worker> worker<TPayload>(
    String topic, {
    String? queueGroup,
    String? durableName,
    int concurrency = 1,
    RetryPolicy? retryPolicy,
    DeadLetterPolicy? deadLetterPolicy,
    required ServerpodJobHandler<TSession, TPayload> handler,
  }) {
    return queue.worker<TPayload>(
      topic,
      queueGroup: queueGroup,
      durableName: durableName,
      concurrency: concurrency,
      retryPolicy: retryPolicy,
      deadLetterPolicy: deadLetterPolicy,
      handler: (context, payload) async {
        final session = await sessionFactory();
        await handler(session, context, payload);
      },
    );
  }
}
