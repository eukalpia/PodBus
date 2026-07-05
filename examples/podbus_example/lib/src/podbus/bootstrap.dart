import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_kafka/podbus_kafka.dart';
import 'package:podbus_nats/podbus_nats.dart';
import 'package:podbus_rabbitmq/podbus_rabbitmq.dart';
import 'package:podbus_serverpod/podbus_serverpod.dart';
import 'package:serverpod/serverpod.dart';

import 'workers.dart';

Future<ServerpodMessagingModule<Session>> createPodBusModule() async {
  final settings = ServerpodMessagingConfigLoader.fromPlatformEnvironment();
  final (:bus, :queue) = _createTransports(settings);
  final messaging = ServerpodMessaging<Session>(
    bus: bus,
    queue: queue,
    sessionFactory: Serverpod.instance.createSession,
    closeSession: (session) => session.close(),
    logger: ServerpodSessionLogAdapter(),
  );

  return ServerpodMessagingModule<Session>(
    messaging: messaging,
    registrations: [registerLeadWorkers],
  );
}

({MessageBus bus, DurableJobQueue queue}) _createTransports(
  ServerpodMessagingSettings settings,
) {
  return switch (settings.transport) {
    ServerpodMessagingTransport.inMemory => (
      bus: InMemoryMessageBus(),
      queue: InMemoryDurableJobQueue(),
    ),
    ServerpodMessagingTransport.nats => _nats(settings),
    ServerpodMessagingTransport.rabbitmq => _rabbitMq(settings),
    ServerpodMessagingTransport.kafka => _kafka(settings),
  };
}

({MessageBus bus, DurableJobQueue queue}) _nats(
  ServerpodMessagingSettings settings,
) {
  final config = NatsMessagingConfig(
    servers: settings.natsServers,
    jetStream: const NatsJetStreamConfig(
      enabled: true,
      streamName: 'PODBUS_EXAMPLE',
      subjects: ['email.>', 'jobs.>'],
      storage: NatsJetStreamStorage.file,
    ),
  );
  return (
    bus: NatsMessageBus(config: config),
    queue: NatsJetStreamJobQueue(config: config),
  );
}

({MessageBus bus, DurableJobQueue queue}) _rabbitMq(
  ServerpodMessagingSettings settings,
) {
  final config = RabbitMqMessagingConfig(
    uri: settings.rabbitMqUrl,
    exchange: settings.rabbitMqExchange,
    deadLetterExchange: settings.rabbitMqDeadLetterExchange,
  );
  final adapter = RabbitMqMessageBus(config: config);
  return (bus: adapter, queue: adapter);
}

({MessageBus bus, DurableJobQueue queue}) _kafka(
  ServerpodMessagingSettings settings,
) {
  final adapter = KafkaEventBus(
    config: KafkaMessagingConfig(
      brokers: settings.kafkaBrokers,
      clientId: settings.kafkaClientId,
      groupId: settings.kafkaGroupId,
      experimental: true,
    ),
  );
  return (bus: adapter, queue: adapter);
}

final class ServerpodSessionLogAdapter
    implements ServerpodMessagingLogger<Session> {
  @override
  void log(ServerpodMessagingLogEntry<Session> entry) {
    entry.session.log(
      entry.message,
      level: switch (entry.level) {
        ServerpodMessagingLogLevel.debug => LogLevel.debug,
        ServerpodMessagingLogLevel.info => LogLevel.info,
        ServerpodMessagingLogLevel.warning => LogLevel.warning,
        ServerpodMessagingLogLevel.error => LogLevel.error,
      },
      exception: entry.error,
      stackTrace: entry.stackTrace,
    );
  }
}
