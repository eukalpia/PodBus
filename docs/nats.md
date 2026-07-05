# NATS

`podbus_nats` currently implements NATS Core through `NatsMessageBus`.

Supported:

- publish/subscribe
- queue groups
- request/reply
- headers
- graceful drain
- health check via `flush`

Not supported yet:

- JetStream stream creation
- durable consumers
- explicit JetStream ack/nak
- JetStream-backed durable jobs
- JetStream dead-letter subjects

NATS Core delivery is at-most-once. Use JetStream for durable work once the adapter is implemented.
