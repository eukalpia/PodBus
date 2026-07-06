import 'package:podbus_core/podbus_core.dart';

final class RabbitMqMessagingConfig {
  RabbitMqMessagingConfig({
    required this.uri,
    required this.exchange,
    required this.deadLetterExchange,
    this.durable = true,
    this.prefetchCount = 10,
    this.connectTimeout = const Duration(seconds: 5),
    this.publisherConfirmTimeout = const Duration(seconds: 5),
  }) {
    if (exchange.trim().isEmpty) {
      throw const MessagingConfigurationException(
        'RabbitMQ exchange must not be empty.',
      );
    }
    if (deadLetterExchange.trim().isEmpty) {
      throw const MessagingConfigurationException(
        'RabbitMQ deadLetterExchange must not be empty.',
      );
    }
    if (prefetchCount < 1) {
      throw const MessagingConfigurationException(
        'RabbitMQ prefetchCount must be greater than zero.',
      );
    }
    if (publisherConfirmTimeout <= Duration.zero) {
      throw const MessagingConfigurationException(
        'RabbitMQ publisherConfirmTimeout must be greater than zero.',
      );
    }
  }

  final Uri uri;
  final String exchange;
  final String deadLetterExchange;
  final bool durable;
  final int prefetchCount;
  final Duration connectTimeout;
  final Duration publisherConfirmTimeout;
}
