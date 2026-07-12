# RabbitMQ publisher-confirm lanes

PodBus uses a small pool of independent AMQP publisher channels for confirmed RabbitMQ publishing.

Each lane allows **one unconfirmed publish at a time**. Publishes are distributed round-robin across lanes, so the application keeps parallel broker confirmations without allowing multiple outstanding confirms to accumulate on one `dart_amqp` channel.

## Why this exists

`dart_amqp` 0.3.1 has an unsafe multi-ack implementation: it iterates the live key view of its pending-confirm map while removing entries from that map. Under sustained concurrent publishing, RabbitMQ may send a cumulative acknowledgement and trigger `ConcurrentModificationError`.

PodBus avoids that path at the adapter boundary instead of claiming an acknowledgement that the client library did not process safely.

The design preserves these guarantees:

- a publish future completes only after the matching broker confirmation;
- mandatory returned messages fail the matching publish;
- a nack fails the matching publish;
- queued publishes are invalidated when the connection epoch changes;
- a failed publish does not poison the lane for later calls;
- reconnect supervision recreates every publisher channel and its exchange topology.

## Configuration

```dart
final bus = RabbitMqMessageBus(
  config: RabbitMqMessagingConfig(
    uri: Uri.parse('amqp://guest:guest@localhost:5672'),
    exchange: 'podbus.events',
    deadLetterExchange: 'podbus.events.dead',
    publisherChannelCount: 4,
    publisherConfirmTimeout: const Duration(seconds: 5),
  ),
);
```

`publisherChannelCount` accepts values from `1` to `64` and defaults to `4`.

## Choosing a lane count

Start with `4` and change it only with measured evidence.

| Workload | Starting point | Reason |
| --- | ---: | --- |
| Low-volume control messages | 1–2 | Fewer channels and simpler broker state |
| General API/event workload | 4 | Balanced default for confirmed publishing |
| Sustained bulk publishing | 8–16 | More confirmations can progress in parallel |
| Highly constrained broker connection | 1 | Correctness-first serialized confirmation path |

More lanes are not free. Every lane creates an AMQP channel, maintains an exchange cache, and participates in reconnect restoration. A value of `64` is a safety ceiling, not a recommendation.

## Capacity testing

Measure at least:

- end-to-end confirmed publish throughput;
- p50/p95/p99 confirmation latency;
- pending application publishes per lane;
- broker channel count;
- publisher nacks and mandatory returns;
- reconnect frequency and recovery duration;
- memory growth during broker partitions;
- queue depth and consumer lag.

Use the repository stress profile as a regression gate, then repeat the test on the actual broker topology, storage, TLS configuration, network, and hardware used in production.

## Failure behavior

When a lane fails, PodBus treats the adapter as unhealthy and fails outstanding publishes rather than silently moving an ambiguous publish to another lane. The resilience wrapper can then recreate the client, all publisher lanes, the consumer channel, active subscriptions, and workers.

This is deliberately conservative. Retrying an ambiguously confirmed publish may create a duplicate, so externally visible consumers must remain idempotent.

## Future removal

The lane pool is useful independently of the upstream bug because it bounds per-channel state and gives explicit parallelism. If a future `dart_amqp` release fixes multi-ack handling, PodBus can evaluate allowing a configurable number of outstanding confirms per lane. That change must be backed by regression and broker-failure evidence, not assumed from a version bump.
