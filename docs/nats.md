# NATS

`podbus_nats` implements NATS Core through `NatsMessageBus` and durable jobs
through `NatsJetStreamJobQueue`.

Supported:

NATS Core:

- publish/subscribe
- queue groups
- request/reply
- headers
- graceful drain
- health check via `flush`

JetStream:

- JetStream stream creation
- durable consumers
- explicit JetStream ack/nak
- JetStream-backed durable jobs
- JetStream dead-letter subjects

Current limitations:

- scheduled enqueue is intentionally unsupported by the JetStream adapter
- delayed retry currently depends on the client adapter behavior and needs
  broker-side delayed NAK coverage before it should be treated as production
  retry infrastructure
- consumer tuning is still small: batch size, ack wait, max pending, max
  deliveries, heartbeat/in-progress behavior, and stream retention knobs need
  broader configuration and tests
- dead-letter payloads and error details should be treated as sensitive data
  unless the application explicitly redacts them

NATS Core delivery is at-most-once. Use JetStream for durable work, and make
handlers idempotent because JetStream processing is at-least-once.
