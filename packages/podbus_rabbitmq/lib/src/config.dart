import 'dart:io';

import 'package:podbus_core/podbus_core.dart';

final class RabbitMqMessagingConfig {
  RabbitMqMessagingConfig({
    required this.uri,
    required this.exchange,
    required this.deadLetterExchange,
    this.retryExchange,
    this.durable = true,
    this.prefetchCount = 10,
    this.publisherChannelCount = 4,
    this.connectTimeout = const Duration(seconds: 5),
    this.publisherConfirmTimeout = const Duration(seconds: 5),
    this.mandatoryPublish = true,
    this.useBrokerRetryQueues = true,
    this.maxConnectionAttempts = 5,
    this.reconnectWaitTime = const Duration(seconds: 2),
    this.connectionName = 'podbus-rabbitmq',
    this.tlsContext,
    this.onBadCertificate,
  }) {
    if (uri.scheme != 'amqp' && uri.scheme != 'amqps') {
      throw const MessagingConfigurationException(
        'RabbitMQ URI scheme must be amqp or amqps.',
      );
    }
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
    if (effectiveRetryExchange.trim().isEmpty) {
      throw const MessagingConfigurationException(
        'RabbitMQ retryExchange must not be empty.',
      );
    }
    if (prefetchCount < 1) {
      throw const MessagingConfigurationException(
        'RabbitMQ prefetchCount must be greater than zero.',
      );
    }
    if (publisherChannelCount < 1 || publisherChannelCount > 64) {
      throw const MessagingConfigurationException(
        'RabbitMQ publisherChannelCount must be between 1 and 64.',
      );
    }
    if (publisherConfirmTimeout <= Duration.zero) {
      throw const MessagingConfigurationException(
        'RabbitMQ publisherConfirmTimeout must be greater than zero.',
      );
    }
    if (maxConnectionAttempts < 1) {
      throw const MessagingConfigurationException(
        'RabbitMQ maxConnectionAttempts must be greater than zero.',
      );
    }
    if (reconnectWaitTime.isNegative) {
      throw const MessagingConfigurationException(
        'RabbitMQ reconnectWaitTime must not be negative.',
      );
    }
    if (connectionName.trim().isEmpty) {
      throw const MessagingConfigurationException(
        'RabbitMQ connectionName must not be empty.',
      );
    }
  }

  final Uri uri;
  final String exchange;
  final String deadLetterExchange;
  final String? retryExchange;
  final bool durable;
  final int prefetchCount;

  /// Number of independent AMQP publisher channels.
  ///
  /// Each channel keeps at most one unconfirmed publish in flight. This avoids
  /// ambiguous multi-ack handling in the current Dart AMQP client while still
  /// allowing confirmed publishes to progress in parallel across lanes.
  final int publisherChannelCount;

  final Duration connectTimeout;
  final Duration publisherConfirmTimeout;
  final bool mandatoryPublish;
  final bool useBrokerRetryQueues;
  final int maxConnectionAttempts;
  final Duration reconnectWaitTime;
  final String connectionName;
  final SecurityContext? tlsContext;
  final bool Function(X509Certificate certificate)? onBadCertificate;

  String get effectiveRetryExchange => retryExchange ?? '$exchange.retry';
}
