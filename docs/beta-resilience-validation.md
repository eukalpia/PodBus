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

## Implemented automation

The broker-backed suite is implemented in `tool/fault_suite.dart` and runs in
`.github/workflows/fault-injection.yml`. It provisions real NATS and RabbitMQ
brokers plus Toxiproxy, then verifies:

- TCP partitions and automatic delegate recreation;
- broker termination between publication and confirmation;
- independent RabbitMQ publisher and consumer channel failures;
- JetStream `ackWait` expiration and redelivery;
- real process death after a side effect but before acknowledgement;
- competing NATS and RabbitMQ worker replicas;
- shutdown while RabbitMQ dead-letter confirmation is pending;
- burst traffic with deliberately slow handlers and strict concurrency bounds.

Run the same suite locally on a Docker host:

```bash
docker compose -f docker-compose.integration.yaml up -d nats rabbitmq toxiproxy
bash tool/ci/wait_for_nats.sh
bash tool/ci/wait_for_rabbitmq.sh
bash tool/ci/wait_for_toxiproxy.sh
dart run tool/fault_suite.dart --profile=smoke
dart run tool/fault_suite.dart --profile=full
```

Each scenario is isolated, restores the brokers and proxies during cleanup, and
writes a machine-readable report to `test-results/fault-suite.json`. The
workflow retains that report, child-process journals, and broker logs.

## Soak testing

`tool/soak_resilience.dart` continuously publishes to NATS JetStream and
RabbitMQ while alternating TCP partitions and broker restarts. It records:

- successfully acknowledged enqueues;
- total and unique deliveries;
- duplicate and redelivery counts;
- missing acknowledged messages;
- delegate recreation counts;
- recovery latency percentiles;
- process RSS growth;
- every injected fault and unexpected error.

The test fails when any enqueue that completed successfully is missing after the
drain period. Scheduled CI runs one hour every week through
`.github/workflows/soak.yml`.

```bash
# Required release evidence
dart run tool/soak_resilience.dart --duration=1h

# Longer dedicated-host runs
dart run tool/soak_resilience.dart --duration=6h
dart run tool/soak_resilience.dart --duration=24h

# Short validation of the harness itself
dart run tool/soak_resilience.dart \
  --duration=2m \
  --allow-short=true \
  --fault-interval=5s
```

GitHub-hosted jobs have a finite execution window, so runs above five hours are
rejected by the hosted workflow and must execute on a dedicated Docker host.
The soak program itself supports durations from one to twenty-four hours.
