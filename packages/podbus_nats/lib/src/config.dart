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
  });

  final bool enabled;
  final String streamName;
  final List<String> subjects;
  final NatsJetStreamStorage storage;
  final NatsJetStreamRetentionPolicy? retentionPolicy;
  final Duration? maxAge;
  final int? maxMsgs;
  final int? replicas;
}

enum NatsJetStreamStorage { file, memory }

enum NatsJetStreamRetentionPolicy { limits, interest, workQueue }
