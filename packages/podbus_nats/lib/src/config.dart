import 'package:podbus_core/podbus_core.dart';

final class NatsMessagingConfig {
  NatsMessagingConfig({
    required this.servers,
    this.username,
    this.password,
    this.token,
    this.useTls = false,
    this.connectTimeout = const Duration(seconds: 5),
    this.requestTimeout = const Duration(seconds: 30),
    this.jetStream,
  }) {
    if (servers.isEmpty) {
      throw const MessagingConfigurationException(
        'NATS configuration requires at least one server URI.',
      );
    }
    if (username != null && password == null) {
      throw const MessagingConfigurationException(
        'NATS password is required when username is set.',
      );
    }
    if (password != null && username == null) {
      throw const MessagingConfigurationException(
        'NATS username is required when password is set.',
      );
    }
  }

  final List<Uri> servers;
  final String? username;
  final String? password;
  final String? token;
  final bool useTls;
  final Duration connectTimeout;
  final Duration requestTimeout;
  final NatsJetStreamConfig? jetStream;
}

final class NatsJetStreamConfig {
  const NatsJetStreamConfig({
    required this.enabled,
    required this.streamName,
    required this.subjects,
    this.storage = NatsJetStreamStorage.file,
    this.retentionPolicy,
    this.maxAge,
    this.maxMsgs,
    this.replicas,
    this.consumerConfig = const NatsJetStreamConsumerConfig(),
  });

  final bool enabled;
  final String streamName;
  final List<String> subjects;
  final NatsJetStreamStorage storage;
  final NatsJetStreamRetentionPolicy? retentionPolicy;
  final Duration? maxAge;
  final int? maxMsgs;
  final int? replicas;
  final NatsJetStreamConsumerConfig consumerConfig;
}

/// Durable pull-consumer limits used by PodBus workers.
final class NatsJetStreamConsumerConfig {
  const NatsJetStreamConsumerConfig({
    this.ackWait = const Duration(seconds: 30),
    this.maxDeliver = -1,
    this.maxAckPending = 1000,
    this.idleHeartbeat,
  });

  /// Time the broker waits for an acknowledgement before redelivery.
  final Duration ackWait;

  /// Maximum delivery attempts. `-1` means unlimited.
  final int maxDeliver;

  /// Maximum number of unacknowledged deliveries for this consumer.
  final int maxAckPending;

  /// Optional broker heartbeat for idle push consumers. Kept here so the wire
  /// configuration remains complete even though PodBus uses pull consumers.
  final Duration? idleHeartbeat;
}

enum NatsJetStreamStorage { file, memory }

enum NatsJetStreamRetentionPolicy { limits, interest, workQueue }
