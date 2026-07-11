import 'package:podbus_core/podbus_core.dart';

final class KafkaMessagingConfig {
  KafkaMessagingConfig({
    required this.brokers,
    required this.clientId,
    required this.groupId,
    this.requestTimeout = const Duration(seconds: 30),
    this.experimental = true,
    this.flushAfterProduce = true,
    this.producerProperties = const {},
    this.consumerProperties = const {},
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
    if (requestTimeout <= Duration.zero) {
      throw const MessagingConfigurationException(
        'Kafka requestTimeout must be greater than zero.',
      );
    }
  }

  final List<String> brokers;
  final String clientId;
  final String groupId;
  final Duration requestTimeout;
  final bool experimental;

  /// Flushes librdkafka after every PodBus publish so a successful Future
  /// means the record reached the producer delivery boundary, not only the
  /// local queue. Disable only when the caller provides an external flush
  /// strategy and accepts the weaker completion contract.
  final bool flushAfterProduce;

  /// Additional librdkafka producer properties, including SASL/TLS settings.
  final Map<String, String> producerProperties;

  /// Additional librdkafka consumer properties, including SASL/TLS settings.
  final Map<String, String> consumerProperties;
}
