import 'package:podbus_core/podbus_core.dart';

final class KafkaMessagingConfig {
  KafkaMessagingConfig({
    required this.brokers,
    required this.clientId,
    required this.groupId,
    this.requestTimeout = const Duration(seconds: 30),
    this.experimental = true,
  }) {
    if (brokers.isEmpty) {
      throw const MessagingConfigurationException(
        'Kafka configuration requires at least one broker.',
      );
    }
    if (clientId.trim().isEmpty) {
      throw const MessagingConfigurationException(
        'Kafka clientId must not be empty.',
      );
    }
    if (groupId.trim().isEmpty) {
      throw const MessagingConfigurationException(
        'Kafka groupId must not be empty.',
      );
    }
  }

  final List<String> brokers;
  final String clientId;
  final String groupId;
  final Duration requestTimeout;
  final bool experimental;
}
