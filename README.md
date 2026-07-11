<p align="center">
  <img src="assets/podbus.png" alt="PodBus" width="760" />
</p>

<p align="center">
  A transport-aware messaging and durable job toolkit for Dart backends and Serverpod.
</p>

<p align="center">
  <a href="https://github.com/eukalpia/PodBus/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/eukalpia/PodBus/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://github.com/eukalpia/PodBus/actions/workflows/security.yml"><img alt="Security" src="https://github.com/eukalpia/PodBus/actions/workflows/security.yml/badge.svg" /></a>
  <a href="LICENSE"><img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" /></a>
  <img alt="Dart SDK" src="https://img.shields.io/badge/Dart-%5E3.12.0-0175C2?logo=dart" />
  <img alt="Status" src="https://img.shields.io/badge/status-alpha-orange" />
</p>

PodBus gives Dart services a compact API for events, request/reply, durable jobs, retries, dead letters, idempotency, health checks, transactional outbox and inbox processing, and Serverpod session handling—without pretending every broker offers the same guarantees.

> **Project status:** alpha. NATS Core and JetStream are the reference implementations. RabbitMQ is available for controlled production evaluation. Kafka remains explicitly experimental while its Dart/librdkafka integration and failure semantics mature.

## Why PodBus

Messaging libraries often hide broker differences until runtime. PodBus exposes a capability set for every transport, documents delivery semantics, and fails early when a requested feature is unsupported.

- One Dart-first contract for messaging and background jobs
- NATS Core pub/sub and request/reply
- NATS JetStream durable workers with ack, NAK, termination, retry, and dead-letter handling
- RabbitMQ publisher confirms, mandatory routing, bounded consumers, durable queues, and dead-letter handling
- Experimental Kafka event-log adapter with manual commits and guarded DLQ ordering
- Typed JSON codecs with schema versions and stable message type names
- PostgreSQL transactional outbox, leased inbox, and persistent idempotency
- W3C trace propagation, bounded Prometheus metrics, redacted JSON logging, and health probes
- Payload and header limits, structured hooks, and detailed health state
- Serverpod lifecycle and per-message session helpers

## Transport support

| Capability | In-memory | NATS Core | NATS JetStream | RabbitMQ | Kafka |
| --- | :---: | :---: | :---: | :---: | :---: |
| Publish / subscribe | ✓ | ✓ | — | ✓ | ✓ |
| Queue groups | ✓ | ✓ | — | ✓ | consumer groups |
| Request / reply | ✓ | ✓ | — | — | — |
| Durable jobs | test-only | — | ✓ | ✓ | ✓ |
| Delayed delivery | process-local | — | broker NAK | TTL/DLX retry | — |
| Automatic retry | ✓ | — | ✓ | ✓ | — |
| Dead-letter handling | ✓ | — | ✓ | ✓ | ✓ |
| Idempotent publish hook | ✓ | — | ✓ | ✓ | producer only |
| Typed codec registry | ✓ | ✓ | ✓ | ✓ | ✓ |
| Status | development | reference | reference | beta | experimental |

The runtime source of truth is `bus.capabilities` or `queue.capabilities`.

## Packages

| Package | Purpose |
| --- | --- |
| `podbus_core` | Contracts, codecs, policies, limits, in-memory implementations |
| `podbus_nats` | NATS Core and JetStream adapters |
| `podbus_rabbitmq` | RabbitMQ events and durable workers |
| `podbus_kafka` | Experimental Kafka adapter |
| `podbus_postgres` | Transactional outbox, inbox leases, persistent idempotency |
| `podbus_observability` | Tracing decorators, Prometheus registry, JSON logs, health probes |
| `podbus_serverpod` | Serverpod lifecycle and session integration |

## Quick start

Use exact tags or commits while the API is alpha:

```yaml
dependencies:
  podbus_core:
    git:
      url: https://github.com/eukalpia/PodBus.git
      ref: v0.1.0-alpha.1
      path: packages/podbus_core
  podbus_nats:
    git:
      url: https://github.com/eukalpia/PodBus.git
      ref: v0.1.0-alpha.1
      path: packages/podbus_nats
```

Start NATS with JetStream:

```bash
docker run --rm -p 4222:4222 -p 8222:8222 nats:2.10 -js -m 8222
```

Publish and subscribe with NATS Core:

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
      print('Lead ${lead['id']}');
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

## Durable jobs with JetStream

```dart
final queue = NatsJetStreamJobQueue(
  config: NatsMessagingConfig(
    servers: [Uri.parse('nats://localhost:4222')],
    jetStream: const NatsJetStreamConfig(
      enabled: true,
      streamName: 'PODBUS_JOBS',
      subjects: ['jobs.>'],
    ),
  ),
);

await queue.connect();

await queue.worker<Map<String, Object?>>(
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

await queue.enqueue(
  'jobs.email.welcome',
  {'email': 'lead@example.com'},
  idempotencyKey: 'welcome-email:lead-42',
);
```

A successful retry or dead-letter publish happens before the original message is acknowledged, terminated, or committed. Error details are truncated, and the original payload is excluded unless explicitly enabled.

## Transactional outbox and inbox

A database mutation followed by a direct publish can leave the database and broker inconsistent. `podbus_postgres` records the business change and outgoing message in one PostgreSQL transaction.

```dart
final pool = Pool<void>.withUrl(databaseUrl);
final outbox = PostgresOutbox(pool);
await outbox.install();

await pool.runTx((transaction) async {
  await transaction.execute(
    Sql.named('INSERT INTO orders (id, total) VALUES (@id, @total)'),
    parameters: {'id': order.id, 'total': order.total},
  );

  await outbox.enqueue(
    transaction,
    'order.created',
    order.toJson(),
    key: order.id,
    headers: MessageHeaders(correlationId: requestId),
  );
});

final relay = PostgresOutboxRelay(
  outbox: outbox,
  bus: bus,
  workerId: 'orders-api-${Platform.localHostname}',
);
await relay.runOnce();
```

Use `PostgresInbox` around externally visible side effects and `PostgresIdempotencyStore` when every service replica must share the same deduplication boundary.

## Typed payloads and schema evolution

```dart
final registry = MessageCodecRegistry()
  ..register<LeadCreated>(
    messageType: 'crm.lead-created',
    schemaVersion: 2,
    encode: (event) => event.toJson(),
    decode: (json, version) {
      final map = json! as Map<String, Object?>;
      return LeadCreated.fromJson(map, schemaVersion: version);
    },
  );

final messagingConfig = MessagingConfig(codecRegistry: registry);
```

The wire metadata carries `messageType` and `schemaVersion`. Decoders receive the incoming version so applications can upcast older payloads deliberately.

## Observability

```dart
final metrics = PrometheusRegistry();
final logs = JsonMessagingLogSink(write: stdout.writeln);
final spans = PodBusTracer(export: otelExporter.add);

final config = MessagingConfig(
  metricHook: metrics.hook,
  logHook: logs.hook,
);

final tracedBus = InstrumentedMessageBus(
  delegate: bus,
  tracer: spans,
  transport: 'nats',
);
```

The tracing decorator propagates W3C `traceparent` and `tracestate`. The Prometheus registry rejects unbounded labels through an allow-list and a maximum series count. JSON logs redact credentials, tokens, payloads, email addresses, and phone fields by default.

Aggregate readiness and liveness without coupling the package to an HTTP framework:

```dart
final probe = PodBusHealthProbe(
  checks: {
    'events': bus.healthCheck,
    'jobs': queue.healthCheck,
  },
);

final ready = await probe.readiness();
response.statusCode = ready.statusCode;
response.write(ready.toJson());
```

## Reliability model

PodBus does not claim exactly-once delivery. Broker-backed workers use **at-least-once** delivery, so handlers must be idempotent.

1. Acknowledge only after the side effect succeeds.
2. Use a persistent idempotency or inbox store shared by every replica.
3. Use a transactional outbox when a database write and publish represent one action.
4. Exclude dead-letter payloads when messages may contain personal or payment data.
5. Check transport capabilities during startup.
6. Treat durable consumer names and wire schemas as public data contracts.

See [Reliability](docs/reliability.md), [Production deployment](docs/production.md), [Runbook](docs/runbook.md), and the [production-readiness audit](docs/production-readiness-audit.md).

## Serverpod

`podbus_serverpod` opens a fresh Serverpod session for every handler and guarantees cleanup. Startup is rollback-safe: if a queue or registration fails, already-opened transports are closed.

```dart
final messaging = ServerpodMessaging<Session>(
  bus: bus,
  queue: queue,
  sessionFactory: () async => pod.createSession(enableLogging: true),
  closeSession: (session) => session.close(),
);

await messaging.start();
await messaging.worker<Map<String, Object?>>(
  'jobs.lead-score',
  concurrency: 4,
  handler: (session, context, payload) async {
    await Lead.db.updateRow(session, score(payload));
  },
);
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

Docker-backed integration tests:

```bash
docker compose -f docker-compose.integration.yaml up -d nats rabbitmq kafka postgres
PODBUS_RUN_INTEGRATION_TESTS=true dart test \
  packages/podbus_nats/test \
  packages/podbus_rabbitmq/test \
  packages/podbus_kafka/test \
  packages/podbus_postgres/test \
  --tags=integration
```

The stress runner is for regression discovery, not universal broker marketing. See [Testing](docs/testing.md).

## Operations and security

- [Production deployment](docs/production.md)
- [Incident runbook](docs/runbook.md)
- [Disaster recovery](docs/disaster-recovery.md)
- [Upgrade guide](docs/upgrading.md)
- [Repository protection](docs/repository-settings.md)
- [Kubernetes example](deploy/kubernetes/podbus-worker.yaml)
- [Prometheus alerts](deploy/prometheus/podbus-alerts.yml)
- [Security policy](SECURITY.md)

## Roadmap

- Stabilize Kafka partition assignment, rebalance, and delivery reports without private dependency APIs
- Expand broker fault injection and long-running soak coverage
- Publish packages after the alpha API and compatibility policy stabilize
- Validate the adapters in multiple independent production deployments before 1.0

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. Report security issues privately using [SECURITY.md](SECURITY.md), not through a public issue.

## License

Apache License 2.0. See [LICENSE](LICENSE).
