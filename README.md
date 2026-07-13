<p align="center">
  <img src="assets/podbus.png" alt="PodBus" width="720" />
</p>

<p align="center">
  Transport-aware messaging and durable jobs for Dart and Serverpod.
</p>

<p align="center">
  <a href="https://github.com/eukalpia/PodBus/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/eukalpia/PodBus/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://github.com/eukalpia/PodBus/actions/workflows/compatibility.yml"><img alt="Compatibility" src="https://github.com/eukalpia/PodBus/actions/workflows/compatibility.yml/badge.svg" /></a>
  <a href="https://github.com/eukalpia/PodBus/actions/workflows/security.yml"><img alt="Security" src="https://github.com/eukalpia/PodBus/actions/workflows/security.yml/badge.svg" /></a>
  <a href="LICENSE"><img alt="Apache 2.0" src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" /></a>
  <img alt="Dart SDK" src="https://img.shields.io/badge/Dart-%5E3.12.0-0175C2?logo=dart" />
  <img alt="Version" src="https://img.shields.io/badge/version-0.1.0--beta.1-blueviolet" />
</p>

PodBus gives Dart services one explicit API for:

- publish/subscribe and request/reply;
- durable workers, retries, and dead letters;
- typed payloads and schema versions;
- PostgreSQL outbox, inbox, and idempotency;
- tracing, bounded metrics, structured logs, and health checks;
- Serverpod lifecycle integration.

It does **not** pretend NATS Core, JetStream, RabbitMQ, and Kafka have identical semantics. Applications can inspect capabilities at startup and fail before serving traffic when a required guarantee is unavailable.

> [!IMPORTANT]
> PodBus `0.1.0-beta.1` is an evidence-backed beta. Public APIs may still change before `1.0.0`. NATS Core, JetStream, and RabbitMQ are included in the beta qualification. Kafka integration remains experimental.

## Qualification snapshot

The beta gate covers one pinned revision with:

- format, analyzer, unit, coverage, security, and package checks;
- Dart `3.12.0` and the current stable SDK;
- Docker-backed NATS, RabbitMQ, Kafka, and PostgreSQL integration tests;
- plain-Dart deployment without Serverpod;
- **3.25 million mandatory transport messages**;
- **12 broker and network fault scenarios**;
- a real **one-hour resilience soak**.

All stress rows below used 256-byte payloads on GitHub-hosted Ubuntu runners with four logical CPUs. They are regression evidence for that environment, not universal throughput promises.

| Transport | Mode | Messages | Result | Throughput |
| --- | --- | ---: | ---: | ---: |
| NATS Core | queue group; isolated publisher and consumers | 1,000,000 | 1,000,000 unique; 0 duplicates | 42,501.6 msg/s |
| JetStream | memory storage; PubAck + manual ack | 250,000 | 250,000 unique; 0 duplicates | 2,928.9 msg/s |
| JetStream | file storage worker; PubAck + manual ack | 250,000 | 250,000 unique; 0 duplicates | 2,371.2 msg/s |
| RabbitMQ | non-persistent; publisher confirms | 1,000,000 | 1,000,000 received | 5,040.8 msg/s |
| RabbitMQ | persistent queue/messages; confirms | 500,000 | 500,000 received | 1,705.5 msg/s |
| RabbitMQ | durable workers; confirms + manual ack | 250,000 | 250,000 received | 1,404.6 msg/s |

Do not rank these rows as one synthetic race. NATS Core, persistent RabbitMQ, and JetStream provide different persistence and acknowledgement contracts.

Read [Beta qualification](docs/beta-qualification.md) for the exact methodology, failure matrix, operational defaults, and known limits.

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

The packages are not published to pub.dev yet. Pin a Git commit when reproducible builds matter.

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

JetStream and RabbitMQ implement `DurableJobQueue`. The source message is acknowledged only after the handler succeeds, or after retry/dead-letter handling reaches its required confirmation boundary.

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
| Maturity | reference | beta | beta | experimental |

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
