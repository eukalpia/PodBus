<p align="center">
  <img src="assets/podbus.png" alt="PodBus" width="760" />
</p>

<p align="center">
  A transport-aware messaging and durable job toolkit for Dart backends and Serverpod.
</p>

<p align="center">
  <a href="https://github.com/eukalpia/PodBus/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/eukalpia/PodBus/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="LICENSE"><img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" /></a>
  <img alt="Dart SDK" src="https://img.shields.io/badge/Dart-%5E3.12.0-0175C2?logo=dart" />
  <img alt="Status" src="https://img.shields.io/badge/status-alpha-orange" />
</p>

PodBus gives Dart services one small API for events, request/reply, durable jobs, retries, dead letters, idempotency, health checks, and Serverpod session handling—without pretending every broker offers the same guarantees.

> **Project status:** alpha. NATS Core and JetStream are the reference implementations. RabbitMQ is suitable for controlled production evaluation. Kafka remains explicitly experimental while its Dart/librdkafka integration matures.

## Why PodBus

Messaging libraries often hide broker differences until runtime. PodBus exposes a capability set for each transport, keeps delivery semantics documented, and fails early when a requested feature is not supported.

- One Dart-first contract for messaging and background jobs
- NATS Core pub/sub and request/reply
- NATS JetStream durable workers with ack, NAK, termination, retry, and dead-letter handling
- RabbitMQ publisher confirms, bounded consumers, durable queues, and dead-letter routing
- Experimental Kafka event-log adapter with manual commits and confirmed producer flushes
- Typed JSON codecs with schema versions and stable message type names
- Payload and header limits, structured logs, metrics hooks, and health details
- Serverpod lifecycle and per-message session helpers

## Transport support

| Capability | In-memory | NATS Core | NATS JetStream | RabbitMQ | Kafka |
| --- | :---: | :---: | :---: | :---: | :---: |
| Publish / subscribe | ✓ | ✓ | — | ✓ | ✓ |
| Queue groups | ✓ | ✓ | — | ✓ | ✓ |
| Request / reply | ✓ | ✓ | — | — | — |
| Durable jobs | test-only | — | ✓ | ✓ | ✓ |
| Delayed delivery | process-local | — | — | — | — |
| Automatic retry | ✓ | — | ✓ | ✓ | — |
| Dead-letter handling | ✓ | — | ✓ | ✓ | ✓ |
| Idempotent publish hook | ✓ | — | ✓ | ✓ | Kafka producer only |
| Typed codec registry | ✓ | ✓ | ✓ | ✓ | ✓ |
| Status | development | reference | reference | beta | experimental |

The runtime source of truth is `bus.capabilities` or `queue.capabilities`.

## Quick start

Add the core package and one transport as path dependencies while the project is in alpha:

```yaml
dependencies:
  podbus_core:
    git:
      url: https://github.com/eukalpia/PodBus.git
      path: packages/podbus_core
  podbus_nats:
    git:
      url: https://github.com/eukalpia/PodBus.git
      path: packages/podbus_nats
```

Start NATS with JetStream:

```bash
docker run --rm -p 4222:4222 nats:2.10 -js
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

A successful dead-letter publish happens before the original message is terminated or committed. Error details are truncated, and the original payload is excluded unless `includeOriginalPayload` is explicitly enabled.

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

The wire metadata carries both `messageType` and `schemaVersion`. Decoders receive the incoming version so applications can upcast older payloads deliberately instead of guessing from JSON shape.

## Reliability model

PodBus does not claim exactly-once delivery. Broker-backed workers use **at-least-once** delivery, so handlers must be idempotent.

Important rules:

1. Acknowledge only after the side effect succeeds.
2. Use an idempotency store shared by every application instance.
3. Use a transactional outbox when a database write and message publish must succeed as one business operation.
4. Keep dead-letter payloads disabled by default when messages may contain personal or payment data.
5. Check transport capabilities during startup for features your service requires.

See [Reliability](docs/reliability.md), [Architecture](docs/architecture.md), and the [production-readiness audit](docs/production-readiness-audit.md).

## Configuration and observability

`MessagingConfig` centralizes the cross-transport behavior:

```dart
final config = MessagingConfig(
  requestTimeout: const Duration(seconds: 10),
  shutdownTimeout: const Duration(seconds: 15),
  limits: const MessagingLimits(
    maxPayloadBytes: 1024 * 1024,
    maxHeaderBytes: 16 * 1024,
  ),
  logHook: structuredLogger.write,
  metricHook: metrics.record,
);
```

Built-in hooks report publish duration, completed jobs, retries, deduplication, dead letters, and transport errors. Health checks distinguish `healthy`, `degraded`, and `unhealthy` states and include worker-loop failures.

## Serverpod

`podbus_serverpod` opens a fresh Serverpod session for each handler and guarantees session cleanup. Startup is rollback-safe: if the job queue or a registration fails, already-opened transports are closed.

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

## Repository layout

```text
packages/
  podbus_core/       contracts, codecs, policies, limits, in-memory transport
  podbus_nats/       NATS Core and JetStream
  podbus_rabbitmq/   RabbitMQ transport
  podbus_kafka/      experimental Kafka adapter
  podbus_serverpod/  Serverpod lifecycle and session integration
examples/
  podbus_example/    server example
  podbus_example_client/
docs/                architecture, reliability, transport and testing notes
```

## Development

```bash
dart pub get
dart format --output=none --set-exit-if-changed .
dart analyze .
dart test packages/podbus_core/test \
  packages/podbus_nats/test \
  packages/podbus_rabbitmq/test \
  packages/podbus_kafka/test \
  packages/podbus_serverpod/test
```

Broker integration tests:

```bash
docker compose -f docker-compose.integration.yaml up -d nats rabbitmq kafka
PODBUS_RUN_INTEGRATION_TESTS=true dart test \
  packages/podbus_nats/test \
  packages/podbus_rabbitmq/test \
  packages/podbus_kafka/test
```

The stress runner is intended for regression discovery, not as a universal broker benchmark. See [Testing](docs/testing.md).

## Roadmap

- PostgreSQL outbox/inbox and persistent idempotency adapters
- Broker-native RabbitMQ retry queues and temporary exclusive subscriptions
- Kafka partition concurrency, rebalance handling, and retry-topic tooling
- OpenTelemetry bridge for traces and metrics
- Package publication after the alpha API stabilizes

## Contributing and security

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. Please report security issues privately using the process in [SECURITY.md](SECURITY.md), not through a public issue.

## License

Apache License 2.0. See [LICENSE](LICENSE).
