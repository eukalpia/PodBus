<p align="center">
  <img src="assets/podbus.png" alt="PodBus" width="720" />
</p>

<p align="center">
  <strong>Transport-aware messaging and durable jobs for Dart and Serverpod.</strong><br />
  Shared application contracts without pretending distinct brokers have identical guarantees.
</p>

<p align="center">
  <a href="https://github.com/eukalpia/PodBus/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/eukalpia/PodBus/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://github.com/eukalpia/PodBus/actions/workflows/fault-injection.yml"><img alt="Fault injection" src="https://github.com/eukalpia/PodBus/actions/workflows/fault-injection.yml/badge.svg" /></a>
  <a href="https://github.com/eukalpia/PodBus/actions/workflows/stress.yml"><img alt="Stress" src="https://github.com/eukalpia/PodBus/actions/workflows/stress.yml/badge.svg" /></a>
  <a href="https://github.com/eukalpia/PodBus/actions/workflows/soak.yml"><img alt="Soak" src="https://github.com/eukalpia/PodBus/actions/workflows/soak.yml/badge.svg" /></a>
  <a href="https://github.com/eukalpia/PodBus/actions/workflows/compatibility.yml"><img alt="Compatibility" src="https://github.com/eukalpia/PodBus/actions/workflows/compatibility.yml/badge.svg" /></a>
  <a href="https://github.com/eukalpia/PodBus/actions/workflows/security.yml"><img alt="Security" src="https://github.com/eukalpia/PodBus/actions/workflows/security.yml/badge.svg" /></a>
  <a href="LICENSE"><img alt="Apache 2.0" src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" /></a>
  <img alt="Dart SDK" src="https://img.shields.io/badge/Dart-%5E3.12.0-0175C2?logo=dart" />
  <img alt="Version" src="https://img.shields.io/badge/version-0.1.0--beta.1-blueviolet" />
  <img alt="Status" src="https://img.shields.io/badge/status-beta-22c55e" />
</p>

PodBus gives Dart services one explicit API for:

- publish/subscribe and request/reply;
- durable workers, retries, and dead letters;
- typed payloads and schema versions;
- PostgreSQL outbox, inbox, and idempotency;
- tracing, bounded metrics, structured logs, and health checks;
- framework-neutral recovery and graceful shutdown;
- optional Serverpod lifecycle integration.

PodBus is **not a broker**. It runs on top of NATS Core, JetStream, RabbitMQ, Kafka, and PostgreSQL reliability primitives. Applications can inspect capabilities at startup and fail before serving traffic when a selected adapter cannot provide a required behavior.

> [!IMPORTANT]
> PodBus `0.1.0-beta.1` is an evidence-backed beta. NATS Core, JetStream, and RabbitMQ are included in the beta qualification. Kafka integration tests are mandatory, but Kafka remains experimental. Public APIs can still change before `1.0.0`.

> [!WARNING]
> Durable processing is at-least-once. PodBus does not claim exactly-once external side effects. Reconnects, consumer restarts, acknowledgement expiry, and ambiguous publish confirmation can produce duplicate delivery. Use an inbox, idempotency key, provider token, or domain uniqueness constraint around externally visible effects.

## Beta qualification snapshot

The beta gate covers one pinned runtime revision with:

- format, analyzer, complete unit suite, coverage, package, and security checks;
- Dart `3.12.0` and the current stable Dart SDK;
- Docker-backed NATS, RabbitMQ, Kafka, and PostgreSQL integration tests;
- plain-Dart deployment outside Serverpod;
- **3.25 million mandatory NATS Core, JetStream, and RabbitMQ messages**;
- **12 isolated broker and network fault scenarios**;
- a real **one-hour NATS and RabbitMQ resilience soak**;
- retained JSON reports, machine metadata, broker logs, and resource snapshots.

Fault and soak harnesses are compiled to AOT executables before qualification. This keeps Dart frontend/kernel-service resources out of process-lifecycle assertions and makes a successful report plus clean process exit the actual gate.

### Measured transport baseline

All rows below used 256-byte payloads on GitHub-hosted Ubuntu runners with four logical CPUs and Dart 3.12.0. They are regression evidence for this environment, not universal throughput promises.

| Transport | Mode and acknowledgement contract | Messages | Result | Elapsed | Throughput |
| --- | --- | ---: | ---: | ---: | ---: |
| NATS Core | queue group; isolated publisher and consumers | 1,000,000 | 1,000,000 unique; 0 duplicates | 23.528 s | **42,501.6 msg/s** |
| JetStream | memory storage; PubAck + manual ack | 250,000 | 250,000 unique; 0 duplicates | 85.356 s | **2,928.9 msg/s** |
| JetStream | file storage worker; PubAck + manual ack | 250,000 | 250,000 unique; 0 duplicates | 105.432 s | **2,371.2 msg/s** |
| RabbitMQ | non-persistent; publisher confirms + manual ack | 1,000,000 | 1,000,000 received | 198.379 s | **5,040.8 msg/s** |
| RabbitMQ | persistent queue/messages; publisher confirms | 500,000 | 500,000 received | 293.165 s | **1,705.5 msg/s** |
| RabbitMQ | durable workers; confirms + manual ack | 250,000 | 250,000 received | 177.982 s | **1,404.6 msg/s** |

Do not rank these rows as one synthetic race. NATS Core, persistent RabbitMQ, and JetStream provide different persistence, confirmation, acknowledgement, and redelivery contracts.

### One-hour disruption soak

The recorded qualification soak ran for **61 minutes 41.859 seconds** and injected **28 faults**: alternating NATS/RabbitMQ TCP partitions and RabbitMQ broker restarts.

| Metric | NATS JetStream | RabbitMQ |
| --- | ---: | ---: |
| Acknowledged enqueues | 25,004 | 25,004 |
| Unique delivered | 25,004 | 25,004 |
| Missing acknowledged messages | **0** | **0** |
| Duplicate deliveries | 14 | 1 |
| Redeliveries | 376 | 0 |
| Delegate factory calls | 71 | 113 |

Additional soak evidence:

- operation errors: **0**;
- recovery latency p50: **967 ms**;
- recovery latency p95: **13.401 s**;
- maximum observed recovery latency: **13.704 s**;
- RSS growth: **54,714,368 bytes** (about **52.2 MiB**);
- configured RSS growth limit: **512 MiB**;
- failure reasons: **none**.

Read [Beta qualification](docs/beta-qualification.md) for the exact harness architecture, failure matrix, operational defaults, and known limits.

## Packages

| Package | Responsibility |
| --- | --- |
| `podbus_core` | Contracts, codecs, policies, limits, resilience wrappers, in-memory implementations |
| `podbus_nats` | NATS Core and JetStream adapters |
| `podbus_rabbitmq` | RabbitMQ messaging and durable workers |
| `podbus_kafka` | Experimental Kafka adapter and native `librdkafka` bindings |
| `podbus_postgres` | Transactional outbox, inbox leases, persistent idempotency |
| `podbus_observability` | Tracing, Prometheus metrics, JSON logs, health probes |
| `podbus_serverpod` | Serverpod lifecycle and per-message session integration |

Packages are not published to pub.dev yet. Pin the beta tag or a reviewed commit for reproducible builds.

```yaml
dependencies:
  podbus_core:
    git:
      url: https://github.com/eukalpia/PodBus.git
      ref: v0.1.0-beta.1
      path: packages/podbus_core
  podbus_nats:
    git:
      url: https://github.com/eukalpia/PodBus.git
      ref: v0.1.0-beta.1
      path: packages/podbus_nats
```

Until the release tag exists, use a reviewed commit SHA instead of the example tag.

## Quick start

Start NATS with JetStream enabled:

```bash
docker run --rm \
  -p 4222:4222 \
  -p 8222:8222 \
  nats:2.10 -js -m 8222
```

Publish and consume an event:

```dart
import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_nats/podbus_nats.dart';

Future<void> main() async {
  final bus = NatsMessageBus(
    config: NatsMessagingConfig(
      servers: [Uri.parse('nats://localhost:4222')],
    ),
  );

  await bus.connect();

  final subscription = await bus.subscribe<Map<String, Object?>>(
    'lead.created',
    queueGroup: 'crm-workers',
    concurrency: 8,
    handler: (_, lead) async {
      print('received lead ${lead['id']}');
    },
  );

  await bus.publish(
    'lead.created',
    {'id': 42, 'email': 'lead@example.com'},
    headers: MessageHeaders(correlationId: 'request-42'),
  );

  await subscription.close();
  await bus.close();
}
```

## Durable workers

JetStream and RabbitMQ implement `DurableJobQueue`. The source delivery is acknowledged only after the handler succeeds, or after retry/dead-letter handling reaches its required confirmation boundary.

```dart
final jobs = NatsJetStreamJobQueue(
  config: NatsMessagingConfig(
    servers: [Uri.parse('nats://localhost:4222')],
    jetStream: const NatsJetStreamConfig(
      enabled: true,
      streamName: 'PODBUS_JOBS',
      subjects: ['jobs.>'],
    ),
  ),
);

await jobs.connect();

await jobs.worker<Map<String, Object?>>(
  'jobs.email.welcome',
  durableName: 'welcome-email-v1',
  concurrency: 8,
  retryPolicy: RetryPolicy(
    maxAttempts: 5,
    initialDelay: const Duration(milliseconds: 250),
    maxDelay: const Duration(seconds: 30),
    jitter: 0.2,
  ),
  deadLetterPolicy: const DeadLetterPolicy(
    enabled: true,
    destination: 'jobs.email.welcome.dead',
    includeErrorDetails: true,
    includeOriginalPayload: false,
  ),
  handler: (_, job) => sendWelcomeEmail(job['email']! as String),
);

await jobs.enqueue(
  'jobs.email.welcome',
  {'email': 'lead@example.com'},
  idempotencyKey: 'welcome-email:lead-42',
);
```

## Delivery model

Broker-backed workers are **at-least-once**. Duplicates are possible after crashes, reconnects, confirmation loss, acknowledgement loss, and lease expiry.

| Concern | PodBus behavior |
| --- | --- |
| Handler success | acknowledge or commit after the handler returns |
| Retryable failure | publish or schedule retry before finalizing the source delivery |
| Terminal failure | confirm dead-letter publication before source finalization |
| Database write + publish | PostgreSQL transactional outbox |
| Duplicate side effects | application idempotency or a shared inbox store |
| Exactly-once external effects | not claimed |

A publish-confirm timeout is ambiguous: the broker may have accepted the message while the acknowledgement was lost. Use stable message IDs and idempotent consumers when retrying.

## Transport choice

| Capability | NATS Core | JetStream | RabbitMQ | Kafka |
| --- | :---: | :---: | :---: | :---: |
| Publish / subscribe | ✓ | — | ✓ | ✓ |
| Request / reply | ✓ | — | — | — |
| Durable workers | — | ✓ | ✓ | ✓ |
| Manual acknowledgement / commit | — | ✓ | ✓ | ✓ |
| Retry and dead letter | — | ✓ | ✓ | experimental |
| Maturity | beta | beta | beta | experimental |

Require capabilities during startup:

```dart
queue.capabilities.requireAll({
  MessagingCapability.durableJobs,
  MessagingCapability.deadLettering,
  MessagingCapability.gracefulShutdown,
});
```

## Reliability primitives

`podbus_postgres` closes the database-write-plus-publish gap with a transactional outbox. Multiple relay replicas use leases and `FOR UPDATE SKIP LOCKED`. A PostgreSQL inbox and persistent idempotency store provide a shared duplicate boundary.

```dart
await pool.runTx((transaction) async {
  await OrderRepository.insert(transaction, order);
  await outbox.enqueue(
    transaction,
    'order.created',
    order.toJson(),
    key: order.id,
    headers: MessageHeaders(correlationId: requestId),
  );
});
```

## Operational design

- RabbitMQ uses a configurable publisher-confirm lane pool with one outstanding confirmation per AMQP channel.
- JetStream uses a concurrent wildcard PubAck inbox with cryptographically random NATS NUID reply subjects.
- JetStream drain is bounded by each original publish deadline.
- Recovery coalesces concurrent reconnect attempts and restores subscriptions/workers.
- Health probes cannot install a replacement delegate after shutdown invalidates their generation.
- NATS and JetStream stress clients run publisher and consumers in independent isolates.
- Payload, header, error-detail, concurrency, and metric-cardinality limits are explicit.
- Graceful shutdown stops new work, drains active handlers/publishes, and closes broker resources.

Read:

- [RabbitMQ publisher-confirm lanes](docs/rabbitmq-publisher-lanes.md)
- [JetStream PubAck operations](docs/jetstream-puback.md)
- [Production deployment](docs/production.md)
- [Reliability model](docs/reliability.md)
- [Incident runbook](docs/runbook.md)
- [Disaster recovery](docs/disaster-recovery.md)
- [Upgrading](docs/upgrading.md)

## Observability

`podbus_observability` provides W3C trace propagation, producer/consumer/worker spans, a bounded-cardinality Prometheus registry, redacted structured JSON logs, and readiness/liveness aggregation.

Do not place message IDs, customer IDs, email addresses, or arbitrary routing keys in Prometheus labels. High-cardinality labels eventually become a monitoring outage wearing a metrics badge.

## Serverpod

`podbus_serverpod` opens a fresh Serverpod session per message and closes it after the handler. Startup is failure-atomic: if registration fails, already-opened transports are closed.

```dart
final messaging = ServerpodMessaging<Session>(
  bus: bus,
  queue: jobs,
  sessionFactory: () => pod.createSession(enableLogging: true),
  closeSession: (session) => session.close(),
);

await messaging.start();
```

## Development

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

Run broker integration:

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

Run fault and soak tools as AOT executables when process lifecycle is part of the assertion:

```bash
dart compile exe tool/fault_suite.dart -o build/podbus-fault-suite
dart compile exe tool/soak_resilience.dart -o build/podbus-soak-resilience

build/podbus-fault-suite --profile=smoke --scenario=nats-tcp-partition
build/podbus-soak-resilience --duration=1h
```

The stress tools are for regression detection and documented capacity work, not for inventing one magical messages-per-second number.

## Before 1.0

The remaining work is deliberately narrower:

- independent production evaluations;
- stable Kafka rebalance, batching, delivery-report, and crash behavior;
- compatibility fixtures for wire-schema evolution;
- repeatable package publication;
- representative broker-cluster and multi-region qualification;
- a documented long-term compatibility policy.

`1.0.0` will mean a stable public API and compatibility policy. It will not mean distributed systems stopped having failure modes.

## Contributing and security

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. Delivery-semantic changes require failure-oriented tests, not only happy-path coverage.

Report vulnerabilities through [SECURITY.md](SECURITY.md), not through a public issue.

## License

Apache License 2.0. See [LICENSE](LICENSE).
