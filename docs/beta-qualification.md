# PodBus 0.1.0-beta.1 qualification

PodBus `0.1.0-beta.1` is an evidence-backed beta for NATS Core, JetStream, and RabbitMQ. Kafka integration remains experimental.

This document records what was tested, how it was tested, and what the results do **not** prove. It does not replace application-specific capacity planning, broker operations, security review, data-governance review, or failure testing against real downstream systems.

## Scope

The qualification covers:

- Dart 3.12.0 and the current stable Dart SDK;
- formatting, analyzer, unit tests, and the repository coverage floor;
- security and package metadata checks;
- NATS Core, NATS JetStream, RabbitMQ, Kafka, and PostgreSQL integration tests;
- a plain-Dart process deployment outside Serverpod;
- 3.25 million mandatory NATS Core, JetStream, and RabbitMQ messages;
- twelve broker and network fault scenarios;
- a one-hour NATS and RabbitMQ resilience soak;
- static website type-check, export, and route validation;
- retained JSON reports, machine metadata, broker logs, and resource snapshots.

Kafka integration tests are mandatory, but Kafka large-stress profiles remain experimental and non-gating. The Kafka adapter is not included in the beta maturity claim.

## Test environment

The transport baseline used GitHub-hosted Ubuntu runners with:

- four logical CPUs;
- Dart 3.12.0;
- clean broker containers per matrix entry;
- 256-byte payloads;
- explicit publisher and consumer concurrency;
- retained runner, kernel, broker, and resource metadata.

Throughput values are regression signals for this environment. They are not promises for arbitrary hardware, replication, disks, network topology, payload size, or acknowledgement policy.

## Mandatory transport evidence

| Transport | Mode | Messages | Result | Elapsed | Throughput |
| --- | --- | ---: | ---: | ---: | ---: |
| NATS Core | queue group, isolated publisher and consumers | 1,000,000 | 1,000,000 unique, 0 duplicates | 23.528 s | 42,501.6 msg/s |
| JetStream | durable, memory storage, PubAck and manual ack | 250,000 | 250,000 unique, 0 duplicates | 85.356 s | 2,928.9 msg/s |
| JetStream | worker, file storage, PubAck and manual ack | 250,000 | 250,000 unique, 0 duplicates | 105.432 s | 2,371.2 msg/s |
| RabbitMQ | non-persistent, confirms and manual ack | 1,000,000 | 1,000,000 received | 198.379 s | 5,040.8 msg/s |
| RabbitMQ | persistent queue/messages, confirms | 500,000 | 500,000 received | 293.165 s | 1,705.5 msg/s |
| RabbitMQ | durable workers, confirms and manual ack | 250,000 | 250,000 received | 177.982 s | 1,404.6 msg/s |

The rows are intentionally not ranked as one benchmark. NATS Core provides a different delivery contract from persistent RabbitMQ or JetStream. Completion under the declared acknowledgement and persistence contract matters more than a synthetic cross-broker leaderboard.

## Harness architecture

### NATS Core

The NATS Core profile uses one publisher isolate and sixteen consumer isolates. Each consumer owns an independent client connection and joins one queue group. A readiness flush completes before publication starts. Consumers report bounded progress and return bitmaps for exact uniqueness verification.

This prevents one publisher loop from starving all consumer sockets on a single Dart event loop.

### NATS JetStream

The JetStream profiles use one publisher isolate and independent consumer isolates attached to one durable pull consumer. Each consumer owns its own NATS connection, fetch loop, bitmap, and manual acknowledgement path.

Publishing uses a PodBus-owned wildcard reply inbox. Every publish receives a unique reply subject, allowing PubAck responses to complete out of order. The implementation preserves message IDs, duplicate metadata, server errors, timeouts, and connection-replacement semantics.

Before the concurrent PubAck path, the 250,000-message durable profile processed only 43,862 messages before a 30-minute timeout. The final profile completed all 250,000 in 85.356 seconds.

### RabbitMQ

RabbitMQ publishing uses a configurable pool of publisher-confirm lanes. Each lane owns an AMQP channel and permits at most one outstanding confirm. Work is distributed round-robin across lanes.

This avoids the unsafe multi-ack mutation path in `dart_amqp 0.3.1` without removing parallel confirmation. A failed or nacked publish does not poison subsequent work on its lane, and reconnect epochs invalidate operations created by an obsolete connection.

## Delivery guarantees demonstrated

The qualification demonstrates the following properties for the tested paths:

- NATS Core completes the tested queue-group workload without broker persistence.
- JetStream enqueue waits for PubAck.
- RabbitMQ publishing waits for AMQP publisher confirmation.
- JetStream and RabbitMQ workers acknowledge only after successful handler completion.
- Persistent RabbitMQ profiles use durable queues and persistent messages.
- Durable workers are at-least-once and may redeliver after failures.
- Source deliveries are not finalized before required retry or dead-letter confirmation boundaries.
- Exactly-once external side effects are not claimed.

## Fault matrix

Every scenario runs in an isolated job with clean NATS, RabbitMQ, and Toxiproxy services:

1. NATS TCP partition and recovery;
2. RabbitMQ TCP partition and recovery;
3. RabbitMQ publisher and consumer channel failures;
4. NATS process crash before acknowledgement;
5. RabbitMQ process crash before acknowledgement;
6. multiple durable-worker replicas;
7. NATS broker stop before publish confirmation;
8. RabbitMQ broker stop before publisher confirmation;
9. NATS shutdown during dead-letter acknowledgement;
10. RabbitMQ shutdown during retry confirmation;
11. RabbitMQ shutdown during dead-letter confirmation;
12. slow consumers and bounded concurrency.

Fault tools are compiled with `dart compile exe` before execution. This keeps Dart frontend/kernel-service resources out of process-lifecycle assertions. A scenario passes only when the AOT process exits successfully and the structured report contains exactly one successful result for the requested scenario.

## One-hour soak evidence

The recorded soak ran for **3,701,859 ms**—61 minutes 41.859 seconds—and injected **28 disruptions**.

Disruptions alternated between:

- NATS TCP partitions;
- RabbitMQ TCP partitions;
- RabbitMQ broker restarts.

### Delivery results

| Metric | NATS JetStream | RabbitMQ |
| --- | ---: | ---: |
| Acknowledged enqueues | 25,004 | 25,004 |
| Deliveries | 25,018 | 25,005 |
| Unique delivered | 25,004 | 25,004 |
| Missing acknowledged messages | **0** | **0** |
| Duplicate deliveries | 14 | 1 |
| Redeliveries | 376 | 0 |
| Maximum attempt | 3 | 1 |
| Delegate factory calls | 71 | 113 |

### Recovery and resource results

- operation errors: **0**;
- recovery latency p50: **967 ms**;
- recovery latency p95: **13,401 ms**;
- maximum recovery latency: **13,704 ms**;
- RSS at start: **255,094,784 bytes**;
- peak RSS: **309,809,152 bytes**;
- RSS growth: **54,714,368 bytes**—about 52.2 MiB;
- configured RSS growth threshold: **536,870,912 bytes**;
- configured recovery p95 threshold: **30,000 ms**;
- failure reasons: **none**.

Soak tools are also compiled to AOT executables. The workflow validates `success == true`, `missing == 0` for both transports, and an empty asynchronous error list before the job can pass.

## Operational defaults

### RabbitMQ publisher lanes

`publisherChannelCount` defaults to `4` and accepts values from `1` through `64`.

Start with four lanes. Increase the value only when publisher-confirm latency is measured as the limiting factor and the RabbitMQ node has channel and connection headroom. More lanes increase channel count, topology recovery work, and in-flight operations during failure.

Use one lane when deterministic ordering on one publishing path matters more than throughput. Do not emulate higher concurrency by allowing several outstanding confirms on one lane while PodBus depends on `dart_amqp 0.3.1`.

Monitor:

- confirm latency and timeout count;
- channel closures and reconnect count;
- unroutable mandatory publishes;
- connection and channel totals per node;
- queue depth, consumer utilisation, and dead-letter growth.

### JetStream publishing

Keep application publish concurrency bounded even though PubAcks can complete concurrently. The transport timeout remains the deadline for each confirmation.

Monitor:

- PubAck latency and timeout count;
- pending publish confirmations;
- stream storage and replica health;
- durable consumer pending and ack-pending counts;
- redelivery count and ack-wait expiration;
- fetch latency and empty-fetch rate.

## Reproducing the gates

Run the ordinary gate:

```bash
dart pub get
dart format --output=none --set-exit-if-changed .
dart analyze .
dart test \
  packages/podbus_core/test \
  packages/podbus_nats/test \
  packages/podbus_rabbitmq/test \
  packages/podbus_kafka/test \
  packages/podbus_postgres/test \
  packages/podbus_observability/test \
  packages/podbus_serverpod/test \
  --exclude-tags=integration
```

Compile lifecycle-sensitive tools:

```bash
mkdir -p build
dart compile exe tool/fault_suite.dart -o build/podbus-fault-suite
dart compile exe tool/soak_resilience.dart -o build/podbus-soak-resilience
```

Run a fault scenario and a full soak:

```bash
build/podbus-fault-suite \
  --profile=smoke \
  --scenario=nats-tcp-partition \
  --report=test-results/nats-partition.json

build/podbus-soak-resilience \
  --duration=1h \
  --report=test-results/soak-summary.json
```

Use the dedicated GitHub workflows for authoritative stress, fault, and soak evidence so every profile receives a clean broker environment, machine metadata, logs, and retained artifacts.

## Known limits before 1.0

- Package versions remain pre-1.0 and public APIs may still change.
- Packages are not yet published to pub.dev.
- Kafka large-stress and rebalance behavior remain experimental.
- Broker clustering and multi-region failure modes require environment-specific qualification.
- Throughput depends on persistence, replication, disk, payload, batching, network, and acknowledgement policy.
- PodBus cannot make non-transactional external side effects exactly-once.
- Applications must validate schemas, authorization, tenant isolation, retention, and data policy for their workloads.

A stable `1.0.0` requires a documented compatibility policy, settled public APIs, repeatable package publication, and independent production experience. It does not require pretending distributed systems stopped having failure modes.
