import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_serverpod/podbus_serverpod.dart';
import 'package:test/test.dart';

void main() {
  group('ServerpodMessaging', () {
    test('wraps message handlers with a session factory', () async {
      final bus = InMemoryMessageBus();
      final queue = InMemoryDurableJobQueue();
      final bridge = ServerpodMessaging<String>(
        bus: bus,
        queue: queue,
        sessionFactory: () async => 'session-1',
      );

      await bridge.start();

      var seenSession = '';
      await bridge.subscribe<Map<String, Object?>>(
        'leads.created',
        handler: (session, context, payload) async {
          seenSession = session;
          await context.ack();
        },
      );

      await bus.publish('leads.created', {'leadId': 7});

      expect(seenSession, 'session-1');
      await bridge.stop();
    });

    test('closes sessions after session-aware handlers', () async {
      final bus = InMemoryMessageBus();
      final queue = InMemoryDurableJobQueue();
      final closed = <String>[];
      final bridge = ServerpodMessaging<String>(
        bus: bus,
        queue: queue,
        sessionFactory: () async => 'session-1',
        closeSession: (session) async => closed.add(session),
      );

      await bridge.start();
      await bridge.subscribe<Map<String, Object?>>(
        'leads.created',
        handler: (_, context, _) async => context.ack(),
      );

      await bus.publish('leads.created', {'leadId': 7});

      expect(closed, ['session-1']);
      await bridge.stop();
    });

    test('logs and closes sessions when handlers fail', () async {
      final bus = ImmediateMessageBus();
      final logger = CapturingServerpodLogger<String>();
      final closed = <String>[];
      final bridge = ServerpodMessaging<String>(
        bus: bus,
        queue: InMemoryDurableJobQueue(),
        sessionFactory: () async => 'session-1',
        closeSession: (session) async => closed.add(session),
        logger: logger,
      );

      await bridge.start();

      await expectLater(
        bridge.subscribe<Map<String, Object?>>(
          'leads.created',
          handler: (_, _, _) async {
            throw StateError('handler failed');
          },
        ),
        throwsStateError,
      );

      expect(closed, ['session-1']);
      expect(logger.entries.single.message, contains('failed'));
      await bridge.stop();
    });
  });

  group('ServerpodMessagingModule', () {
    test('starts transports and runs registrations', () async {
      final bus = InMemoryMessageBus();
      final queue = InMemoryDurableJobQueue();
      final bridge = ServerpodMessaging<String>(
        bus: bus,
        queue: queue,
        sessionFactory: () async => 'session-1',
      );
      var registered = false;
      final module = ServerpodMessagingModule<String>(
        messaging: bridge,
        registrations: [
          (messaging) async {
            registered = true;
            await messaging.subscribe<Map<String, Object?>>(
              'leads.created',
              handler: (_, context, _) async => context.ack(),
            );
          },
        ],
      );

      await module.start();

      expect(registered, isTrue);
      expect((await bus.healthCheck()).status, HealthStatus.healthy);
      await module.stop();
    });
  });

  group('ServerpodMessagingConfigLoader', () {
    test('loads transport settings from environment values', () {
      final settings = ServerpodMessagingConfigLoader.fromEnvironment({
        'PODBUS_TRANSPORT': 'rabbitmq',
        'PODBUS_RABBITMQ_URL': 'amqp://guest:guest@localhost:5672',
        'PODBUS_RABBITMQ_EXCHANGE': 'podbus.events',
        'PODBUS_RABBITMQ_DEAD_LETTER_EXCHANGE': 'podbus.dead',
      });

      expect(settings.transport, ServerpodMessagingTransport.rabbitmq);
      expect(
        settings.rabbitMqUrl,
        Uri.parse('amqp://guest:guest@localhost:5672'),
      );
      expect(settings.rabbitMqExchange, 'podbus.events');
      expect(settings.rabbitMqDeadLetterExchange, 'podbus.dead');
    });
  });
}

final class CapturingServerpodLogger<TSession>
    implements ServerpodMessagingLogger<TSession> {
  final entries = <ServerpodMessagingLogEntry<TSession>>[];

  @override
  Future<void> log(ServerpodMessagingLogEntry<TSession> entry) async {
    entries.add(entry);
  }
}

final class ImmediateMessageBus implements MessageBus {
  var connected = false;

  @override
  Future<void> close({Duration? timeout}) async {
    connected = false;
  }

  @override
  Future<void> connect() async {
    connected = true;
  }

  @override
  Future<HealthCheckResult> healthCheck() async {
    return connected
        ? HealthCheckResult.healthy()
        : HealthCheckResult.unhealthy();
  }

  @override
  Future<void> publish<T>(
    String subject,
    T payload, {
    MessageHeaders? headers,
  }) async {}

  @override
  Future<TResponse> request<TRequest, TResponse>(
    String subject,
    TRequest payload, {
    MessageHeaders? headers,
    Duration? timeout,
  }) {
    throw UnimplementedError();
  }

  @override
  Future<Subscription> subscribe<T>(
    String subject, {
    String? queueGroup,
    required MessageHandler<T> handler,
  }) async {
    await handler(_ImmediateMessageContext(subject), <String, Object?>{} as T);
    return _NoopSubscription();
  }
}

final class _ImmediateMessageContext implements MessageContext {
  _ImmediateMessageContext(this.subject);

  @override
  final String subject;

  @override
  MessageHeaders get headers => MessageHeaders();

  @override
  Object? get rawMessage => null;

  @override
  Future<void> ack() async {}

  @override
  Future<void> extendVisibility(Duration duration) async {}

  @override
  Future<void> nak({Duration? delay}) async {}

  @override
  Future<void> reply<T>(T payload, {MessageHeaders? headers}) async {}

  @override
  Future<void> terminate() async {}
}

final class _NoopSubscription implements Subscription {
  @override
  Future<void> close() async {}
}
