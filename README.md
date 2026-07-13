<p align="center">
  <img src="assets/podbus.png" alt="PodBus" width="720" />
</p>

<p align="center">
  Messaging and durable jobs for Dart, without flattening every broker into the same abstraction.
</p>

<p align="center">
  <a href="https://github.com/eukalpia/PodBus/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/eukalpia/PodBus/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://github.com/eukalpia/PodBus/actions/workflows/compatibility.yml"><img alt="Compatibility" src="https://github.com/eukalpia/PodBus/actions/workflows/compatibility.yml/badge.svg" /></a>
  <a href="https://github.com/eukalpia/PodBus/actions/workflows/security.yml"><img alt="Security" src="https://github.com/eukalpia/PodBus/actions/workflows/security.yml/badge.svg" /></a>
  <a href="LICENSE"><img alt="Apache 2.0" src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" /></a>
  <img alt="Dart SDK" src="https://img.shields.io/badge/Dart-%5E3.12.0-0175C2?logo=dart" />
  <img alt="Status" src="https://img.shields.io/badge/status-beta%20candidate-blueviolet" />
</p>

PodBus is a Dart toolkit for message-driven services. It provides a common API for publish/subscribe, request/reply, durable workers, retries, dead letters, typed payloads, health checks, and Serverpod integration.

The abstraction stops where broker semantics diverge. Applications can inspect transport capabilities at startup instead of discovering an unsupported guarantee after deployment.

PodBus is not a broker. It runs on top of NATS, RabbitMQ, Kafka, and PostgreSQL-backed reliability primitives.

> [!IMPORTANT]
> PodBus packages remain `0.1.0-alpha.1`, but the NATS Core, JetStream, and RabbitMQ paths are now an evidence-backed **beta candidate**. Public APIs may still change before the first stable release. Kafka integration is covered by CI, while Kafka large-stress and rebalance behavior remain experimental.

## Qualification snapshot

The beta-candidate gate is pinned to commit `784b3d533f4ea962324394c31ea4712c1a5e8c47` and includes CI, security, plain-Dart deployment, Dart 3.12 plus current-stable compatibility, 3.25 million mandatory transport messages, twelve broker fault scenarios, and a real one-hour resilience soak.

All stress payloads were 256 bytes on GitHub-hosted Ubuntu runners with four logical CPUs. These values are regression evidence for that environment—not universal performance promises.

| Transport | Mode | Messages | Result | Throughput |
| --- | --- | ---: | ---: | ---: |
| NATS Core | queue group, isolated publisher and consumers | 1,000,000 | 1,000,000 unique, 0 duplicates | 42,501.6 msg/s |
| JetStream | durable, memory storage, PubAck and manual ack | 250,000 | 250,000 unique, 0 duplicates | 2,928.9 msg/s |
| JetStream | worker, file storage, PubAck and manual ack | 250,000 | 250,000 unique, 0 duplicates | 2,371.2 msg/s |
| RabbitMQ | non-persistent, publisher confirms | 1,000,000 | 1,000,000 received | 5,040.8 msg/s |
| RabbitMQ | persistent queue and messages, confirms | 500,000 | 500,000 received | 1,705.5 msg/s |
| RabbitMQ | durable workers, confirms and manual ack | 250,000 | 250,000 received | 1,404.6 msg/s |

Do not rank these rows as one synthetic race. NATS Core, persistent RabbitMQ, and JetStream provide different persistence and acknowledgement contracts. Read [Beta candidate qualification](docs/beta-qualification.md) for methodology, failure evidence, operational defaults, and known limits.

## What is included

- NATS Core publish/subscribe and request/reply
- NATS JetStream durable workers and concurrent PubAck routing
- RabbitMQ publisher-confirm lanes, mandatory routing, retries, and dead-letter queues
- Experimental Kafka producers and consumer groups through native `librdkafka` bindings
- Typed JSON codecs with explicit message types and schema versions
- PostgreSQL transactional outbox, inbox leases, and persistent idempotency
- W3C trace-context propagation, Prometheus metrics, structured logs, and health probes
- Serverpod lifecycle and per-message session helpers
- Bounded concurrency, payload limits, graceful shutdown, and transport health reporting

## Quick start

The packages are not published to pub.dev yet. The example below tracks `main`; pin a commit in any project where reproducible builds matter.

```yaml
dependencies:
  podbus_core:
    git:
      url: https://github.com/eukalpia/PodBus.git
      ref: main
      path: packages/podbus_core
  podbus_nats:
    git:
      url: https://github.com/eukalpia/PodBus.git
      ref: main
      path: packages/podbus_nats
```

Start NATS with JetStream enabled:

```bash
docker run --rm \
  -p 4222:4222 \
  -p 8222:8222 \
  nats:2.10 -js -m 8222
```

Publish an event and consume it through a queue group:

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
    handler: (context, lead) async {
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

## Durable jobs

JetStream and RabbitMQ implement the `DurableJobQueue` contract. A worker acknowledges a job only after the handler completes successfully.

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
  handler: (context, job) async {
    await sendWelcomeEmail(job['email']! as String);
  },
);

await jobs.enqueue(
  'jobs.email.welcome',
  {'email': 'lead@example.com'},
  idempotencyKey: 'welcome-email:lead-42',
);
```

Retries and dead-letter publication complete before the source message is acknowledged, terminated, or committed. That protects delivery state; it does not make an arbitrary business side effect exactly-once.

## Choosing a transport

| Capability | In-memory | NATS Core | NATS JetStream | RabbitMQ | Kafka |
| --- | :---: | :---: | :---: | :---: | :---: |
| Publish / subscribe | ✓ | ✓ | — | ✓ | ✓ |
| Queue groups | ✓ | ✓ | — | ✓ | consumer groups |
| Request / reply | ✓ | ✓ | — | — | — |
| Durable workers | test only | — | ✓ | ✓ | ✓ |
| Delayed retry | process local | — | broker NAK | TTL / DLX | — |
| Dead-letter handling | ✓ | — | ✓ | ✓ | ✓ |
| Manual acknowledgement or commit | — | — | ✓ | ✓ | ✓ |
| Typed codec registry | ✓ | ✓ | ✓ | ✓ | ✓ |
| Maturity | development | reference | reference | beta candidate | experimental |

Use `capabilities` as the runtime source of truth:

```dart
queue.capabilities.requireAll({
  MessagingCapability.durableJobs,
  MessagingCapability.deadLettering,
  MessagingCapability.gracefulShutdown,
});
```

A practical rule of thumb:

- **NATS Core** for low-latency events and request/reply where broker persistence is not required.
- **NATS JetStream** for durable work with explicit consumer state and redelivery.
- **RabbitMQ** for queue-oriented workloads, routing, publisher confirms, and broker-managed retry topology.
- **Kafka** for append-only event streams and consumer groups, while accepting that the current adapter is still experimental.

## Delivery and consistency

PodBus uses **at-least-once delivery** for broker-backed workers. Duplicate delivery is expected during failures, reconnects, lease expiry, and consumer restarts.

| Concern | PodBus behavior |
| --- | --- |
| Handler success | acknowledge or commit after the handler returns |
| Handler failure | classify, retry, or dead-letter according to policy |
| Duplicate delivery | application-level idempotency or a shared inbox store |
| Database write plus publish | PostgreSQL transactional outbox |
| Ordering | determined by the selected broker and partitioning strategy |
| Exactly-once side effects | not claimed |

For business operations, treat idempotency keys, durable consumer names, and message schemas as persistent data contracts rather than implementation details.

## Transactional outbox and inbox

A database transaction followed by a broker publish is not atomic. `podbus_postgres` records the business change and outgoing message in the same PostgreSQL transaction.

```dart
final pool = Pool<void>.withUrl(databaseUrl);
final outbox = PostgresOutbox(pool);

await outbox.install();

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

`PostgresOutboxRelay` publishes pending records with leases and `FOR UPDATE SKIP LOCKED`, allowing multiple relay replicas to share the same table. `PostgresInbox` and `PostgresIdempotencyStore` provide a shared deduplication boundary for consumers.

See [Reliability](docs/reliability.md) for the full failure model.

## Typed messages

The codec registry keeps Dart types separate from stable wire names:

```dart
final codecs = MessageCodecRegistry()
  ..register<LeadCreated>(
    messageType: 'crm.lead-created',
    schemaVersion: 2,
    encode: (event) => event.toJson(),
    decode: (json, version) {
      return LeadCreated.fromJson(
        json! as Map<String, Object?>,
        schemaVersion: version,
      );
    },
  );
```

The envelope carries `messageType` and `schemaVersion`. Consumers receive the incoming version and can upcast older payloads deliberately. Unknown future versions are rejected rather than guessed.

## Observability

`podbus_observability` is deliberately framework-neutral. It provides:

- W3C `traceparent` and `tracestate` propagation
- producer, consumer, request, and worker spans
- a bounded-cardinality Prometheus registry
- JSON logs with credential and personal-data redaction
- readiness and liveness aggregation

```dart
final metrics = PrometheusRegistry(maxSeries: 2000);
final logs = JsonMessagingLogSink(write: stdout.writeln);
final tracer = PodBusTracer(export: spanExporter.add);

final config = MessagingConfig(
  metricHook: metrics.hook,
  logHook: logs.hook,
);

final tracedBus = InstrumentedMessageBus(
  delegate: bus,
  tracer: tracer,
  transport: 'nats',
);
```

Message IDs, tenant IDs, email addresses, and arbitrary routing keys should not be Prometheus labels. The registry uses an allow-list and a hard series limit because an observability layer should not become the next outage.

## Serverpod

`podbus_serverpod` opens a fresh Serverpod session for each message and closes it after the handler finishes. Startup is failure-atomic: if registration fails, already-opened transports are closed.

```dart
final messaging = ServerpodMessaging<Session>(
  bus: bus,
  queue: jobs,
  sessionFactory: () => pod.createSession(enableLogging: true),
  closeSession: (session) => session.close(),
);

await messaging.start();
```

## Packages

| Package | Responsibility |
| --- | --- |
| `podbus_core` | Contracts, codecs, policies, limits, in-memory implementations |
| `podbus_nats` | NATS Core and JetStream adapters |
| `podbus_rabbitmq` | RabbitMQ messaging and durable workers |
| `podbus_kafka` | Experimental Kafka adapter and native `librdkafka` bindings |
| `podbus_postgres` | Transactional outbox, inbox leases, persistent idempotency |
| `podbus_observability` | Tracing, Prometheus metrics, JSON logs, health probes |
| `podbus_serverpod` | Serverpod lifecycle and session integration |

## Production guidance

The repository includes operational material alongside the library:

- [Beta candidate qualification](docs/beta-qualification.md)
- [Production deployment](docs/production.md)
- [Incident runbook](docs/runbook.md)
- [Disaster recovery](docs/disaster-recovery.md)
- [Upgrade guide](docs/upgrading.md)
- [Production-readiness audit](docs/production-readiness-audit.md)
- [Kubernetes worker example](deploy/kubernetes/podbus-worker.yaml)
- [Prometheus alert rules](deploy/prometheus/podbus-alerts.yml)
- [Repository protection](docs/repository-settings.md)

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

Run broker-backed tests locally:

```bash
docker compose -f docker-compose.integration.yaml up -d nats rabbitmq kafka postgres

PODBUS_RUN_INTEGRATION_TESTS=true dart test \
  packages/podbus_nats/test \
  packages/podbus_rabbitmq/test \
  packages/podbus_kafka/test \
  packages/podbus_postgres/test \
  --tags=integration
```

Benchmarks in distributed systems are configuration-dependent. The stress tools in `tool/` are intended for regression detection and capacity work on a documented environment, not for claiming one universal messages-per-second number.

## Project status

Before the first stable release, the work is focused on:

- independent production evaluations and feedback from real workloads
- stable Kafka rebalance, batching, and delivery-report behavior
- compatibility fixtures for wire-schema evolution
- repeatable package publication after the API surface settles
- broker-cluster and multi-region failure qualification in representative environments

A `1.0.0` release will mean a documented compatibility policy and a stable public API. It will not mean exactly-once delivery across arbitrary external side effects.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. Changes to delivery semantics, wire formats, or durable consumer behavior should include failure-oriented tests, not only happy-path coverage.

## Security

Report vulnerabilities through the process described in [SECURITY.md](SECURITY.md). Do not disclose security issues in a public GitHub issue.

## License

PodBus is licensed under the [Apache License 2.0](LICENSE).
