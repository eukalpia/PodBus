# RabbitMQ publisher-confirm lanes

PodBus uses independent AMQP publisher channels to preserve parallel publisher
confirmation without entering the unsafe multi-ack mutation path in
`dart_amqp 0.3.1`.

## Design

`RabbitMqMessagingConfig.publisherChannelCount` accepts values from `1` through
`64` and defaults to `4`.

Each lane owns one AMQP channel and allows at most one outstanding publisher
confirmation on that channel. Calls are assigned round-robin across lanes. A
nack, returned mandatory message, timeout, or channel failure completes the
current operation but does not poison later work assigned to the lane.

When a connection is replaced, PodBus increments the connection epoch. Queued
operations from an obsolete epoch fail rather than being published through a
new connection under stale assumptions.

## Starting configuration

Use the default of four lanes until measurements show confirmation latency is
the publisher bottleneck.

```dart
final bus = RabbitMqMessageBus(
  config: RabbitMqMessagingConfig(
    uri: Uri.parse('amqps://podbus@rabbit.internal:5671/app'),
    exchange: 'podbus.events',
    deadLetterExchange: 'podbus.dead',
    mandatoryPublish: true,
    publisherChannelCount: 4,
    publisherConfirmTimeout: const Duration(seconds: 5),
  ),
);
```

Use one lane when deterministic ordering on one publishing path is more
important than throughput. Increase the lane count only after testing the exact
payload, persistence, routing, replication, and disk configuration used in
production.

More lanes are not free. They increase:

- AMQP channel count per application replica;
- topology recovery work after reconnect;
- the number of simultaneous in-flight operations during broker failure;
- broker and client memory used for confirmation state.

Do not increase concurrency by permitting multiple outstanding confirmations on
one lane while PodBus depends on `dart_amqp 0.3.1`. That recreates the failure
this design prevents.

## Capacity calculation

For `R` application replicas and `L` publisher lanes per replica, budget at
least `R × L` publisher channels, plus consumer and topology-management
channels.

Example: twelve replicas with eight lanes require 96 publisher channels before
counting consumers. Verify the RabbitMQ channel policy, memory headroom, and
connection limits before rollout.

## Metrics and alerts

Monitor:

- publisher-confirm latency percentiles;
- confirmation timeout and nack count;
- returned mandatory messages;
- publisher and consumer channel closures separately;
- reconnect count and time to restore topology;
- total channels and connections per broker node;
- queue depth, consumer utilisation, redelivery, and dead-letter growth;
- broker memory, disk alarms, and flow control.

An increase in lane count is successful only when end-to-end throughput improves
without unacceptable confirmation latency, broker pressure, or recovery time.

## Failure semantics

A successful confirmation means the broker accepted the publish according to
RabbitMQ confirmation semantics. It does not mean a consumer processed the
message.

A timeout is ambiguous: the broker may have accepted the message while the
confirmation was lost. Retrying can therefore produce a duplicate. Use stable
idempotency keys, a durable inbox, or an application-specific deduplication
boundary when duplicate side effects are unsafe.

Mandatory publishing detects unroutable messages. Keep it enabled for durable
business events unless silent routing loss is explicitly acceptable.

## Removing the workaround

Do not remove the one-confirm-per-channel rule merely because a newer
`dart_amqp` version exists. First prove all of the following against the exact
candidate version:

1. concurrent confirms do not mutate pending state during iteration;
2. multiple acknowledgements and nacks complete the correct futures;
3. returned mandatory messages remain correlated with the correct publish;
4. channel closure fails every affected operation exactly once;
5. reconnect does not carry stale pending state into the new channel;
6. the million-message confirmed stress profile and fault matrix pass.

Only then should PodBus consider a simpler shared-channel implementation. A
smaller diff is not automatically a safer transport.