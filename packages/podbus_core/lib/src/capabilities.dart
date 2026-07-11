import 'exceptions.dart';

enum MessagingCapability {
  publishSubscribe,
  queueGroups,
  requestReply,
  manualAcknowledgement,
  negativeAcknowledgement,
  termination,
  visibilityExtension,
  durableJobs,
  delayedDelivery,
  retries,
  deadLettering,
  idempotentPublish,
  typedPayloads,
  gracefulShutdown,
}

final class MessagingCapabilities {
  const MessagingCapabilities(this.values);

  static const none = MessagingCapabilities(<MessagingCapability>{});

  final Set<MessagingCapability> values;

  bool supports(MessagingCapability capability) => values.contains(capability);

  bool supportsAll(Iterable<MessagingCapability> capabilities) {
    return capabilities.every(values.contains);
  }

  void requireAll(Iterable<MessagingCapability> capabilities) {
    final missing = <MessagingCapability>[
      for (final capability in capabilities)
        if (!values.contains(capability)) capability,
    ];
    if (missing.isEmpty) {
      return;
    }
    throw MessagingUnsupportedException(
      'Transport does not support required capabilities: '
      '${missing.map((capability) => capability.name).join(', ')}.',
    );
  }
}

abstract interface class MessagingCapabilityProvider {
  MessagingCapabilities get capabilities;
}
