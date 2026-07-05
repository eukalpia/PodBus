# Production Readiness Audit

Date: 2026-07-06

This audit combines local Docker-backed stress runs with a static review of the
transport adapters, example app, and documentation. It is not a benchmark and
does not claim production capacity.

## Stress Smoke Results

Environment:

- macOS local development machine
- Docker services from `docker-compose.integration.yaml`
- NATS `2.10`
- RabbitMQ `3.13-management`
- Kafka `apache/kafka:3.9.0`
- default local broker settings

Command:

```bash
PODBUS_STRESS_MESSAGES=2000 PODBUS_STRESS_CONCURRENCY=100 dart run tool/stress_transports.dart
```

Results:

| Transport | Received | Total elapsed | Approx throughput |
| --- | ---: | ---: | ---: |
| NATS Core | 2000 / 2000 | 70 ms | 28287.7 msg/s |
| NATS JetStream | 2000 / 2000 | 352 ms | 5670.2 msg/s |
| RabbitMQ | 2000 / 2000 | 78 ms | 25401.3 msg/s |
| Kafka | 2000 / 2000 | 2960 ms | 675.6 msg/s |

Command:

```bash
PODBUS_STRESS_MESSAGES=10000 PODBUS_STRESS_CONCURRENCY=250 dart run tool/stress_transports.dart
```

Results:

| Transport | Received | Total elapsed | Approx throughput |
| --- | ---: | ---: | ---: |
| NATS Core | 10000 / 10000 | 174 ms | 57327.9 msg/s |
| NATS JetStream | 10000 / 10000 | 1430 ms | 6992.0 msg/s |
| RabbitMQ | 10000 / 10000 | 259 ms | 38601.4 msg/s |
| Kafka | 10000 / 10000 | 4256 ms | 2349.2 msg/s |

Command:

```bash
PODBUS_STRESS_MESSAGES=100000 PODBUS_STRESS_CONCURRENCY=500 dart run tool/stress_transports.dart
```

Results:

| Transport | Received | Total elapsed | Approx throughput |
| --- | ---: | ---: | ---: |
| NATS Core | 100000 / 100000 | 928 ms | 107655.6 msg/s |
| NATS JetStream | 100000 / 100000 | 13755 ms | 7269.9 msg/s |
| RabbitMQ | 100000 / 100000 | 2182 ms | 45813.1 msg/s |
| Kafka | 100000 / 100000 | 17188 ms | 5817.8 msg/s |

No message loss was observed in these three local happy-path runs. The stress
tool does not yet test broker restarts, network partitions, malformed messages,
consumer crashes, disk pressure, authentication failures, or multi-node broker
clusters.

## Highest Priority Issues

1. Kafka can commit past a failed record.

   A failed handler is recorded as an error, but the consumer loop can continue
   polling. If a later record from the same partition succeeds and commits, the
   failed offset can be skipped. This breaks the documented "failed handler does
   not commit" behavior and is the first Kafka correctness issue to fix.

2. RabbitMQ retry and dead-letter republish are not confirmed before acking the
   source delivery.

   The adapter publishes retry or dead-letter messages and then acknowledges the
   original delivery, but it does not use publisher confirms or mandatory-return
   handling. A broker-side publish failure can become data loss.

3. NATS JetStream idempotency is claimed before publish is confirmed.

   If enqueue claims an idempotency key and encode or publish fails afterward,
   a retry with the same key can be suppressed until the idempotency entry
   expires.

4. Malformed broker headers can bypass normal failure handling.

   NATS JetStream and RabbitMQ parse headers before the guarded processing path
   in important places. Bad `attempt` or retry-policy headers can leave messages
   unacked and repeatedly redelivered.

5. NATS Core event handlers run without bounded backpressure.

   Core subscriptions dispatch handler futures without tracking active work.
   Under load this can create unbounded in-flight processing, unhandled async
   errors, and shutdown that does not wait for active handlers.

6. Kafka producer publish currently means "queued locally", not "delivered".

   The native adapter does not expose per-message delivery reports. Broker-side
   delivery failures after local enqueue are not visible to callers.

7. `amqps://` RabbitMQ configuration does not yet prove TLS is active.

   The config accepts a URI, but the adapter needs explicit `amqp`/`amqps`
   validation, TLS setup, and default port handling.

8. Example Serverpod endpoints expose internal messaging operations.

   The example client can call methods that publish events and enqueue jobs.
   That is acceptable for a demo only if documented and guarded before any
   production-oriented example is published.

## Transport Backlog

### NATS

- Track in-flight handler futures and surface handler failures.
- Add bounded concurrency or explicit backpressure for Core subscriptions.
- Move JetStream header parsing into the guarded failure path.
- Make delayed NAK broker-side, not a client-side sleep that holds a worker.
- Add idempotency pending/commit/release semantics or rely on JetStream
  `messageId` after publish acknowledgement.
- Expose consumer tuning for ack wait, max pending, max deliveries, batch
  fetch, heartbeat/in-progress, stream retention, and redelivery policy.
- Redact and cap dead-letter error details.
- Add tests for malformed headers, failed ack/nak/term, publish failure after
  idempotency claim, delayed NAK, close under active handlers, and long-running
  job redelivery.

### RabbitMQ

- Add explicit TLS support for `amqps://`.
- Add publisher confirms and mandatory-return handling.
- Ack original deliveries only after retry or dead-letter publish is confirmed.
- Move header decoding into guarded processing and handle malformed messages.
- Replace delayed retry sleeps with TTL retry queues or the delayed-message
  plugin.
- Use server-named exclusive auto-delete queues for anonymous subscriptions.
- Fix queue naming collisions by using reversible encoding or a hash suffix.
- Define one dead-letter exchange and routing-key contract.
- Track connection/channel close events and rebuild topology on reconnect.
- Add integration tests for broker-side DLX, confirms, queue cleanup,
  reconnection, malformed headers, and prefetch versus concurrency.

### Kafka

- Fix per-partition commit ordering so failed offsets cannot be skipped.
- Add delivery report handling or document an explicit fire-and-forget mode.
- Add security config for TLS/SASL and validated librdkafka properties.
- Reject relative `PODBUS_LIBRDKAFKA_PATH` values and document the variable as
  trusted startup configuration only.
- Handle malformed envelopes through a raw-byte quarantine or dead-letter path.
- Honor `includeOriginalPayload` for dead-letter behavior.
- Classify librdkafka errors such as queue full, unknown topic, authorization
  failure, and fatal producer/consumer errors.
- Cache topic handles and reduce per-message copy overhead.
- Keep the adapter experimental until these semantics are tested with real
  brokers.

### Serverpod Example And Bridge

- Add auth, scopes, rate limits, and stricter input validation to example
  endpoints before presenting them as production patterns.
- Make startup failure-atomic: if bus, queue, worker, or subscription setup
  fails, close whatever already opened.
- Catch and log session close failures without masking handler exceptions.
- Fix the generated Serverpod integration test helper import mismatch.
- Keep local default broker credentials documented as development-only.
- Avoid logging emails, headers, or full stack traces in examples.

## Production Checklist Before Release

- All transport adapters pass malformed-message tests.
- Durable paths prove "publish confirmed before source ack" where the broker
  supports it.
- Health checks include connection state and last fatal worker/subscription
  errors.
- Shutdown drains active handlers or force-closes with explicit timeout
  behavior.
- Broker auth/TLS examples exist for NATS, RabbitMQ, and Kafka.
- Dead-letter docs explain sensitive data handling.
- Stress tests include failure cases, not only happy-path throughput.
- README and package docs do not claim exactly-once delivery or high-load
  readiness.
