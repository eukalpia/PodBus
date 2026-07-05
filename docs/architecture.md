# Architecture

PodBus is split into small Dart packages:

- `podbus_core`: stable transport-agnostic contracts.
- `podbus_nats`: NATS Core adapter and JetStream durable job queue.
- `podbus_rabbitmq`: RabbitMQ adapter package.
- `podbus_kafka`: experimental Kafka event-log adapter.
- `podbus_serverpod`: optional Serverpod bridge.

The core package owns behavior that every transport must respect: headers, codecs, retry policy, dead-letter policy, idempotency, health checks, logging hooks, and metrics hooks.

Transport packages must not leak broker client APIs through `MessageBus` or `DurableJobQueue`. When an adapter needs a client seam for testing, it should be a PodBus-owned interface.
