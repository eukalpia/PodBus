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
}

abstract interface class MessagingCapabilityProvider {
  MessagingCapabilities get capabilities;
}
