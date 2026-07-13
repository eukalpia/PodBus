# Beta candidate qualification

This document records the qualification evidence for PodBus commit
`784b3d533f4ea962324394c31ea4712c1a5e8c47`.

PodBus is a **beta candidate**, not a claim of universal production readiness.
The evidence below proves the listed code paths on the listed environment. It
does not replace application-specific load testing, capacity planning, broker
operations, security review, or failure testing against real downstream
systems.

## Scope

The qualification covers:

- Dart 3.12.0 and the current stable Dart SDK;
- unit tests and the repository coverage floor;
- NATS Core, NATS JetStream, RabbitMQ, Kafka, and PostgreSQL integration tests;
- plain-Dart deployment without Serverpod;
- static analysis, formatting, package metadata, and security workflows;
- 3.25 million mandatory transport messages;
- twelve broker and network fault scenarios;
- a one-hour NATS and RabbitMQ resilience soak.

Kafka integration tests are mandatory, but Kafka large-stress profiles remain
experimental and non-gating. The current Kafka adapter is not included in the
beta maturity claim.

## Test environment

The authoritative transport run used GitHub-hosted Ubuntu runners with four
logical CPUs and Dart 3.12.0. Each matrix entry started a clean broker container
and captured:

- runner CPU, memory, kernel, and Dart metadata;
- the full stress result;
- broker logs;
- post-run container resource state;
- RabbitMQ connection, channel, queue, and exchange state where applicable.

Every result is tied to the same commit SHA. Throughput values are regression
signals for this environment, not promises for arbitrary hardware or broker
configurations.

## Mandatory transport evidence

All payloads were 256 bytes.

| Transport | Mode | Messages | Result | Elapsed | Throughput |
| --- | --- | ---: | ---: | ---: | ---: |
| NATS Core | queue group, isolated publisher and consumers | 1,000,000 | 1,000,000 unique, 0 duplicates | 23.528 s | 42,501.6 msg/s |
| JetStream | durable, memory storage, PubAck and manual ack | 250,000 | 250,000 unique, 0 duplicates | 85.356 s | 2,928.9 msg/s |
| JetStream | worker, file storage, PubAck and manual ack | 250,000 | 250,000 unique, 0 duplicates | 105.432 s | 2,371.2 msg/s |
| RabbitMQ | non-persistent fast path, confirms and manual ack | 1,000,000 | 1,000,000 received | 198.379 s | 5,040.8 msg/s |
| RabbitMQ | persistent queue and messages, confirms | 500,000 | 500,000 received | 293.165 s | 1,705.5 msg/s |
| RabbitMQ | durable worker path, confirms and manual ack | 250,000 | 250,000 received | 177.982 s | 1,404.6 msg/s |

These rows are intentionally not ranked as one benchmark. NATS Core provides a
very different delivery contract from persistent RabbitMQ or JetStream. The
result that matters is completion with the expected acknowledgement and
persistence semantics.

## Harness architecture

### NATS Core

The NATS Core profile runs one publisher isolate and sixteen consumer isolates.
Each consumer owns an independent client connection and joins one queue group.
A readiness flush completes before publication starts. Consumers report bounded
progress to the coordinator and return one bitmap each for exact uniqueness
verification.

This design prevents a publisher loop from starving every consumer socket on a
single Dart event loop.

### NATS JetStream

The JetStream profiles run one publisher isolate and independent consumer
isolates attached to one durable pull consumer. Each consumer owns its own NATS
connection, fetch loop, bitmap, and manual acknowledgement path. The first
consumer creates the durable; the remaining consumers attach after readiness to
avoid a consumer-creation race.

The publisher uses a PodBus-owned wildcard reply inbox. Each publish receives a
unique reply subject, allowing JetStream PubAck responses to complete out of
order. This avoids the global request/reply mutex in `dart_nats 1.1.1` while
preserving message IDs, timeouts, duplicate metadata, server errors, and close
semantics.

Before this change, the same 250,000-message durable profile processed only
43,862 messages before the 30-minute timeout. The final profile completed all
250,000 messages in 85.356 seconds.

### RabbitMQ

RabbitMQ publishing uses a configurable pool of publisher-confirm lanes. Each
lane owns an AMQP channel and permits at most one outstanding confirm on that
channel. Work is distributed round-robin across lanes.

This avoids the unsafe multi-ack mutation path in `dart_amqp 0.3.1` without
removing parallelism. A failed or nacked operation does not poison subsequent
work on the lane, and reconnect epochs invalidate operations from an obsolete
connection.

## Delivery guarantees demonstrated

The qualification demonstrates the following properties for the tested paths:

- NATS Core delivers the tested queue-group workload without broker persistence.
- JetStream and RabbitMQ workers acknowledge only after successful handler completion.
- JetStream enqueue waits for PubAck.
- RabbitMQ publish waits for AMQP publisher confirmation.
- Persistent RabbitMQ profiles use durable queues and persistent messages.
- Durable worker paths are at-least-once and may redeliver after failures.
- Duplicate-safe business side effects still require application idempotency or a shared inbox store.
- Exactly-once behavior across arbitrary external side effects is not claimed.

## Fault matrix

The broker fault workflow runs every scenario in an isolated job with clean
NATS, RabbitMQ, and Toxiproxy services:

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

A scenario passes only when its expected delivery, failure, redelivery,
reconnection, or confirmation behavior is observed. Broker logs and structured
JSON evidence are retained as workflow artifacts.

## Soak gate

The resilience soak runs NATS and RabbitMQ continuously for one hour with
periodic disruption and recovery. The release gate requires:

- no missing acknowledged messages;
- no unhandled asynchronous errors;
- bounded recovery after disruption;
- successful shutdown and evidence upload;
- retained broker logs for post-run inspection.

A shorter detector-only workflow is not accepted as soak evidence.

## Operational defaults

### RabbitMQ publisher lanes

`publisherChannelCount` defaults to `4` and accepts values from `1` through
`64`.

Start with four lanes. Increase the value only when publisher-confirm latency is
measured as the limiting factor and the RabbitMQ node has channel and connection
headroom. More lanes increase broker channel count, topology recovery work, and
in-flight operations during failure.

Use one lane when deterministic ordering on one publishing path matters more
than throughput. Do not emulate higher concurrency by allowing multiple
outstanding confirms on one lane while PodBus depends on `dart_amqp 0.3.1`.

Monitor:

- confirm latency and timeout count;
- channel closures and reconnect count;
- unroutable mandatory publishes;
- connection and channel totals per node;
- queue depth, consumer utilisation, and dead-letter growth.

### JetStream publishing

Keep the application publish concurrency bounded even though PubAcks can now
complete concurrently. The transport timeout remains the upper bound for each
confirmation. Monitor:

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

Run broker integration locally:

```bash
docker compose -f docker-compose.integration.yaml up -d \
  nats rabbitmq kafka postgres

PODBUS_RUN_INTEGRATION_TESTS=true dart test \
  packages/podbus_nats/test \
  packages/podbus_rabbitmq/test \
  packages/podbus_kafka/test \
  packages/podbus_postgres/test \
  --tags=integration
```

Run the large transport profiles through the `Large transport stress` GitHub
workflow so each profile gets a clean broker, isolated runner, metadata, logs,
and retained evidence. Run fault and soak qualification through their dedicated
workflows rather than combining them into an unrepeatable local command.

## Known limits before 1.0

- Package versions remain pre-1.0 and public APIs may still change.
- Packages are not yet published to pub.dev.
- Kafka large-stress and rebalance behavior remain experimental.
- Broker clustering and multi-region failure modes require environment-specific testing.
- Throughput depends on persistence, replication, disk, payload, batching, network, and acknowledgement policy.
- PodBus cannot make non-transactional external side effects exactly-once.
- Applications must validate schemas, authorization, tenant isolation, retention, and data policy for their own workloads.

A stable `1.0.0` release requires a documented compatibility policy, settled
public APIs, repeatable package publication, and independent production
experience. It does not require pretending distributed systems stopped having
failure modes.