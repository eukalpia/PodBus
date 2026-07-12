# NATS and RabbitMQ beta resilience validation

This document defines the evidence required before the NATS and RabbitMQ adapters can be described as beta-ready.

## Delivery contract

PodBus uses at-least-once delivery for durable work. Reconnect and redelivery can produce duplicates. Business handlers must therefore use an inbox, an idempotency key, or a domain-level uniqueness constraint for externally visible side effects.

A source delivery must not be acknowledged until a retry or dead-letter publication has completed with the transport's delivery confirmation. A failed confirmation leaves the source delivery unacknowledged so the broker can redeliver it.

## Required deterministic tests

- reconnect after publish or enqueue connection loss;
- restoration of active subscriptions and durable workers;
- coalescing concurrent reconnect attempts;
- bounded reconnect attempts with exponential backoff and jitter;
- no reconnect for application failures;
- degraded health during recovery;
- long-running JetStream handlers protected by `inProgress` heartbeats;
- duplicate redelivery preserving the delivery attempt;
- strict handler concurrency under burst load;
- RabbitMQ retry confirmation before source acknowledgement;
- RabbitMQ dead-letter confirmation before source acknowledgement;
- graceful shutdown waiting for active handlers and requeuing buffered deliveries.

## Required broker-backed fault tests

The release gate must exercise the following against real broker processes rather than mocks:

1. terminate and restart NATS while publishers and durable consumers remain active;
2. terminate and restart RabbitMQ while publishers and workers remain active;
3. interrupt the TCP path during publish and restore it during reconnect backoff;
4. close RabbitMQ publisher and consumer channels independently;
5. exceed the JetStream acknowledgement window and verify redelivery;
6. stop a consumer after receiving a message but before acknowledgement;
7. run multiple replicas sharing the same durable consumer or queue group;
8. inject duplicate deliveries and verify an application idempotency guard;
9. stop the process while retry or dead-letter publication is awaiting confirmation;
10. sustain slow handlers and burst publishers without exceeding configured concurrency or memory bounds.

## Release evidence

A beta declaration requires:

- clean formatting and analyzer output on Dart 3.12 and current stable;
- all unit tests passing without skips;
- NATS and RabbitMQ integration suites passing independently;
- fault tests passing repeatedly in CI;
- broker logs and machine-readable test reports retained as artifacts;
- a minimum one-hour soak with no lost acknowledged messages;
- a documented duplicate count and recovery-time distribution;
- successful graceful shutdown under active load;
- a controlled deployment outside Serverpod, proving the API works in a plain Dart process.

Passing unit tests alone is not sufficient evidence for production readiness.
