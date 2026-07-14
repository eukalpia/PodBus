# podbus_nats

NATS Core and JetStream adapters for PodBus.

Use NATS Core for low-latency publish/subscribe and request/reply. Use JetStream for durable workers, explicit acknowledgements, retries, redelivery, and dead-letter handling.

## Status

`0.1.0-beta.1` is beta-qualified for NATS Core and JetStream. Delivery remains at-least-once and applications must make externally visible side effects idempotent.

## Install

```bash
dart pub add podbus_nats
dart pub add podbus_core
```

Start NATS with JetStream enabled:

```bash
docker run --rm -p 4222:4222 -p 8222:8222 nats:2.10 -js -m 8222
```

See the [PodBus quick start](https://github.com/eukalpia/PodBus#quick-start) and [JetStream PubAck guide](https://github.com/eukalpia/PodBus/blob/main/docs/jetstream-puback.md).
