// ignore_for_file: prefer_initializing_formals

import 'dart:async';
import 'dart:io';

import 'package:podbus_core/podbus_core.dart';

typedef ServerpodSessionFactory<TSession> = Future<TSession> Function();

typedef ServerpodSessionCloser<TSession> =
    FutureOr<void> Function(TSession session);

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

typedef ServerpodMessagingRegistration<TSession> =
    Future<void> Function(ServerpodMessaging<TSession> messaging);

final class ServerpodMessaging<TSession> {
  ServerpodMessaging({
    required this.bus,
    required this.queue,
    required this.sessionFactory,
    ServerpodSessionCloser<TSession>? closeSession,
    ServerpodMessagingLogger<TSession>? logger,
  }) : _closeSession = closeSession,
       _logger = logger ?? const NoopServerpodMessagingLogger();

  final MessageBus bus;
  final DurableJobQueue queue;
  final ServerpodSessionFactory<TSession> sessionFactory;
  final ServerpodSessionCloser<TSession>? _closeSession;
  final ServerpodMessagingLogger<TSession> _logger;

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
        await _withSession(
          operation: 'message handler for $subject',
          run: (session) => handler(session, context, payload),
        );
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
        await _withSession(
          operation: 'job handler for $topic',
          run: (session) => handler(session, context, payload),
        );
      },
    );
  }

  Future<void> _withSession({
    required String operation,
    required Future<void> Function(TSession session) run,
  }) async {
    final session = await sessionFactory();
    try {
      await run(session);
    } on Object catch (error, stackTrace) {
      await _logger.log(
        ServerpodMessagingLogEntry<TSession>(
          session: session,
          level: ServerpodMessagingLogLevel.error,
          message: 'PodBus $operation failed.',
          error: error,
          stackTrace: stackTrace,
        ),
      );
      rethrow;
    } finally {
      await _closeSession?.call(session);
    }
  }
}

final class ServerpodMessagingModule<TSession> {
  ServerpodMessagingModule({
    required this.messaging,
    this.registrations = const [],
  });

  final ServerpodMessaging<TSession> messaging;
  final List<ServerpodMessagingRegistration<TSession>> registrations;
  var _started = false;

  Future<void> start() async {
    if (_started) {
      return;
    }
    await messaging.start();
    for (final registration in registrations) {
      await registration(messaging);
    }
    _started = true;
  }

  Future<void> stop({Duration? timeout}) async {
    if (!_started) {
      return;
    }
    _started = false;
    await messaging.stop(timeout: timeout);
  }
}

abstract interface class ServerpodMessagingLogger<TSession> {
  FutureOr<void> log(ServerpodMessagingLogEntry<TSession> entry);
}

final class NoopServerpodMessagingLogger<TSession>
    implements ServerpodMessagingLogger<TSession> {
  const NoopServerpodMessagingLogger();

  @override
  FutureOr<void> log(ServerpodMessagingLogEntry<TSession> entry) {}
}

final class ServerpodMessagingLogEntry<TSession> {
  const ServerpodMessagingLogEntry({
    required this.session,
    required this.level,
    required this.message,
    this.error,
    this.stackTrace,
  });

  final TSession session;
  final ServerpodMessagingLogLevel level;
  final String message;
  final Object? error;
  final StackTrace? stackTrace;
}

enum ServerpodMessagingLogLevel { debug, info, warning, error }

final class ServerpodMessagingConfigLoader {
  static ServerpodMessagingSettings fromPlatformEnvironment() {
    return fromEnvironment(Platform.environment);
  }

  static ServerpodMessagingSettings fromEnvironment(
    Map<String, String> environment,
  ) {
    final transport = switch ((environment['PODBUS_TRANSPORT'] ?? 'nats')
        .toLowerCase()) {
      'nats' => ServerpodMessagingTransport.nats,
      'rabbitmq' || 'rabbit' => ServerpodMessagingTransport.rabbitmq,
      'kafka' => ServerpodMessagingTransport.kafka,
      'memory' || 'in_memory' => ServerpodMessagingTransport.inMemory,
      final value => throw MessagingConfigurationException(
        'Unsupported PODBUS_TRANSPORT value "$value".',
      ),
    };

    return ServerpodMessagingSettings(
      transport: transport,
      natsServers:
          _uriList(environment['PODBUS_NATS_SERVERS']) ??
          [
            Uri.parse(
              environment['PODBUS_NATS_URL'] ?? 'nats://localhost:4222',
            ),
          ],
      rabbitMqUrl: Uri.parse(
        environment['PODBUS_RABBITMQ_URL'] ??
            'amqp://guest:guest@localhost:5672',
      ),
      rabbitMqExchange:
          environment['PODBUS_RABBITMQ_EXCHANGE'] ?? 'podbus.events',
      rabbitMqDeadLetterExchange:
          environment['PODBUS_RABBITMQ_DEAD_LETTER_EXCHANGE'] ?? 'podbus.dead',
      kafkaBrokers:
          _stringList(environment['PODBUS_KAFKA_BROKERS']) ??
          [environment['PODBUS_KAFKA_BROKER'] ?? 'localhost:9092'],
      kafkaClientId:
          environment['PODBUS_KAFKA_CLIENT_ID'] ?? 'podbus-serverpod',
      kafkaGroupId: environment['PODBUS_KAFKA_GROUP_ID'] ?? 'podbus-serverpod',
    );
  }

  static List<Uri>? _uriList(String? value) {
    final values = _stringList(value);
    if (values == null) {
      return null;
    }
    return [for (final item in values) Uri.parse(item)];
  }

  static List<String>? _stringList(String? value) {
    if (value == null || value.trim().isEmpty) {
      return null;
    }
    return [
      for (final item in value.split(','))
        if (item.trim().isNotEmpty) item.trim(),
    ];
  }
}

final class ServerpodMessagingSettings {
  const ServerpodMessagingSettings({
    required this.transport,
    required this.natsServers,
    required this.rabbitMqUrl,
    required this.rabbitMqExchange,
    required this.rabbitMqDeadLetterExchange,
    required this.kafkaBrokers,
    required this.kafkaClientId,
    required this.kafkaGroupId,
  });

  final ServerpodMessagingTransport transport;
  final List<Uri> natsServers;
  final Uri rabbitMqUrl;
  final String rabbitMqExchange;
  final String rabbitMqDeadLetterExchange;
  final List<String> kafkaBrokers;
  final String kafkaClientId;
  final String kafkaGroupId;
}

enum ServerpodMessagingTransport { nats, rabbitmq, kafka, inMemory }
