import type { DocPage } from '@/lib/docs-types';

export const gettingStartedDocs: DocPage[] = [
  {
    slug: 'introduction',
    title: 'Introduction',
    description:
      'What PodBus is, where it fits, and the design constraints behind its API.',
    category: 'Getting started',
    order: 1,
    sections: [
      {
        id: 'what-is-podbus',
        title: 'What PodBus is',
        blocks: [
          {
            type: 'paragraph',
            text: 'PodBus is a Dart toolkit for message-driven services. It provides common contracts for publish/subscribe, request/reply, durable jobs, retries, dead letters, typed payloads, health checks, observability, and Serverpod integration.',
          },
          {
            type: 'paragraph',
            text: 'The project does not try to erase meaningful differences between brokers. NATS Core, JetStream, RabbitMQ, and Kafka have different persistence, ordering, routing, acknowledgement, and recovery models. PodBus exposes those differences through transport capabilities and transport-specific configuration.',
          },
          {
            type: 'note',
            tone: 'info',
            title: 'PodBus is not a broker',
            text: 'It is a library that runs inside your Dart service and talks to existing infrastructure. Broker durability, replication, retention, and quotas still need to be configured on the broker itself.',
          },
        ],
      },
      {
        id: 'design-goals',
        title: 'Design goals',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Keep application code small without hiding delivery semantics.',
              'Fail during startup when a required capability is unavailable.',
              'Treat duplicate delivery as a normal failure mode, not an edge case.',
              'Make retries, dead letters, timeouts, and shutdown behavior explicit.',
              'Provide database-backed reliability patterns for business-critical work.',
              'Stay usable in plain Dart services and integrate cleanly with Serverpod.',
            ],
          },
        ],
      },
      {
        id: 'project-status',
        title: 'Project status',
        blocks: [
          {
            type: 'table',
            headers: ['Area', 'Status', 'Meaning'],
            rows: [
              ['Core contracts', 'Alpha', 'The API is usable but may still change before 1.0.'],
              ['NATS Core', 'Reference', 'Primary implementation for events and request/reply.'],
              ['NATS JetStream', 'Reference', 'Primary implementation for durable workers.'],
              ['RabbitMQ', 'Beta', 'Suitable for controlled production evaluation.'],
              ['Kafka', 'Experimental', 'Use only with explicit acceptance of adapter limitations.'],
              ['PostgreSQL reliability', 'Alpha', 'Outbox, inbox, and persistent idempotency are available.'],
            ],
          },
          {
            type: 'note',
            tone: 'warning',
            title: 'Pin the version',
            text: 'Until packages are published and the public API stabilizes, pin a Git commit or release tag. Do not depend on a moving branch in a production build.',
          },
        ],
      },
      {
        id: 'mental-model',
        title: 'Mental model',
        blocks: [
          {
            type: 'paragraph',
            text: 'A PodBus application usually has three layers: application handlers, a PodBus contract, and a transport adapter. Reliability helpers such as outbox, inbox, idempotency, tracing, and health checks sit beside the transport rather than pretending to be broker features.',
          },
          {
            type: 'code',
            language: 'text',
            filename: 'architecture.txt',
            code: `Application service
  ├─ MessageBus              events + request/reply
  ├─ DurableJobQueue         acknowledged background work
  ├─ Codec registry          wire types + schema versions
  ├─ Policies                retry + dead letter + limits
  └─ Reliability helpers     outbox + inbox + idempotency
             │
             ▼
Transport adapter
  ├─ NATS Core
  ├─ NATS JetStream
  ├─ RabbitMQ
  └─ Kafka`,
          },
        ],
      },
    ],
  },
  {
    slug: 'installation',
    title: 'Installation',
    description:
      'Add only the packages required by your service and pin a reproducible revision.',
    category: 'Getting started',
    order: 2,
    sections: [
      {
        id: 'package-layout',
        title: 'Package layout',
        blocks: [
          {
            type: 'table',
            headers: ['Package', 'Use it for'],
            rows: [
              ['podbus_core', 'Contracts, codecs, policies, limits, and in-memory implementations.'],
              ['podbus_nats', 'NATS Core events and JetStream durable workers.'],
              ['podbus_rabbitmq', 'RabbitMQ routing, confirms, retries, and dead letters.'],
              ['podbus_kafka', 'Experimental Kafka producers and consumer groups.'],
              ['podbus_postgres', 'Transactional outbox, inbox leases, persistent idempotency.'],
              ['podbus_observability', 'Tracing, metrics, structured logs, and health probes.'],
              ['podbus_serverpod', 'Serverpod lifecycle and per-message session handling.'],
            ],
          },
        ],
      },
      {
        id: 'git-dependencies',
        title: 'Install from Git',
        blocks: [
          {
            type: 'paragraph',
            text: 'Packages are currently consumed from the repository. Replace the example commit with the revision you have tested. All PodBus packages in one application should come from the same revision.',
          },
          {
            type: 'code',
            language: 'yaml',
            filename: 'pubspec.yaml',
            code: `dependencies:
  podbus_core:
    git:
      url: https://github.com/eukalpia/PodBus.git
      ref: <commit-sha>
      path: packages/podbus_core

  podbus_nats:
    git:
      url: https://github.com/eukalpia/PodBus.git
      ref: <commit-sha>
      path: packages/podbus_nats`,
          },
          {
            type: 'code',
            language: 'bash',
            code: 'dart pub get',
          },
        ],
      },
      {
        id: 'native-dependencies',
        title: 'Native dependencies',
        blocks: [
          {
            type: 'paragraph',
            text: 'Most adapters are pure Dart. The Kafka adapter uses native librdkafka bindings and therefore requires the matching shared library at runtime.',
          },
          {
            type: 'table',
            headers: ['Platform', 'Typical package'],
            rows: [
              ['Ubuntu / Debian', 'librdkafka-dev or librdkafka1'],
              ['macOS', 'brew install librdkafka'],
              ['Windows', 'A compatible rdkafka.dll on PATH'],
            ],
          },
          {
            type: 'note',
            tone: 'warning',
            title: 'Kafka remains experimental',
            text: 'A successfully loaded native library does not prove delivery, rebalance, or crash behavior. Run the integration and fault tests against the same broker and librdkafka versions used in production.',
          },
        ],
      },
      {
        id: 'version-discipline',
        title: 'Version discipline',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Commit pubspec.lock for applications.',
              'Use the same PodBus revision across all workspace packages.',
              'Review CHANGELOG.md before upgrading.',
              'Run unit and broker-backed integration tests before deployment.',
              'Treat wire schema changes separately from package upgrades.',
            ],
          },
        ],
      },
    ],
  },
  {
    slug: 'quick-start',
    title: 'Quick start',
    description:
      'Publish an event, consume it through a queue group, and close cleanly.',
    category: 'Getting started',
    order: 3,
    sections: [
      {
        id: 'start-nats',
        title: 'Start NATS',
        blocks: [
          {
            type: 'paragraph',
            text: 'The fastest way to exercise both events and durable jobs locally is a NATS server with JetStream enabled.',
          },
          {
            type: 'code',
            language: 'bash',
            code: `docker run --rm \
  -p 4222:4222 \
  -p 8222:8222 \
  nats:2.10 -js -m 8222`,
          },
        ],
      },
      {
        id: 'publish-subscribe',
        title: 'Publish and subscribe',
        blocks: [
          {
            type: 'code',
            language: 'dart',
            filename: 'bin/main.dart',
            code: `import 'package:podbus_core/podbus_core.dart';
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
      print('lead: \${lead['id']}');
      print('correlation: \${context.headers.correlationId}');
    },
  );

  await bus.publish(
    'lead.created',
    {'id': 42, 'email': 'lead@example.com'},
    headers: MessageHeaders(correlationId: 'request-42'),
  );

  await subscription.close();
  await bus.close();
}`,
          },
          {
            type: 'note',
            tone: 'info',
            title: 'Queue groups distribute work',
            text: 'Subscribers using the same queue group compete for each message. Subscribers without a queue group each receive their own copy.',
          },
        ],
      },
      {
        id: 'durable-worker',
        title: 'Add a durable worker',
        blocks: [
          {
            type: 'code',
            language: 'dart',
            code: `final jobs = NatsJetStreamJobQueue(
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

final worker = await jobs.worker<Map<String, Object?>>(
  'jobs.email.welcome',
  durableName: 'welcome-email-v1',
  concurrency: 8,
  retryPolicy: RetryPolicy(
    maxAttempts: 5,
    initialDelay: const Duration(milliseconds: 250),
    maxDelay: const Duration(seconds: 30),
    jitter: 0.2,
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

await worker.close();
await jobs.close();`,
          },
        ],
      },
      {
        id: 'shutdown',
        title: 'Shut down in the right order',
        blocks: [
          {
            type: 'steps',
            items: [
              {
                title: 'Stop accepting new traffic',
                description: 'Mark readiness unhealthy and stop new HTTP or RPC requests.',
              },
              {
                title: 'Stop fetching new messages',
                description: 'Close subscriptions and workers so no additional handlers start.',
              },
              {
                title: 'Drain active handlers',
                description: 'Wait for in-flight work until the configured shutdown deadline.',
              },
              {
                title: 'Close the transport',
                description: 'Flush pending publishes and release broker connections.',
              },
            ],
          },
        ],
      },
    ],
  },
  {
    slug: 'configuration',
    title: 'Configuration',
    description:
      'Set shared limits, hooks, timeouts, codecs, and transport-specific options.',
    category: 'Getting started',
    order: 4,
    sections: [
      {
        id: 'shared-configuration',
        title: 'Shared messaging configuration',
        blocks: [
          {
            type: 'paragraph',
            text: 'MessagingConfig holds behavior shared by adapters: codec registration, size limits, default retry behavior, request timeouts, logging, metrics, and failure classification.',
          },
          {
            type: 'code',
            language: 'dart',
            code: `final messagingConfig = MessagingConfig(
  codecRegistry: codecs,
  limits: const MessagingLimits(
    maxPayloadBytes: 1024 * 1024,
    maxHeaderBytes: 16 * 1024,
  ),
  requestTimeout: const Duration(seconds: 5),
  metricHook: metrics.hook,
  logHook: logs.hook,
);`,
          },
        ],
      },
      {
        id: 'limits',
        title: 'Payload and header limits',
        blocks: [
          {
            type: 'paragraph',
            text: 'Limits protect consumers and brokers from accidental memory pressure and oversized diagnostic metadata. Set them from the smallest safe value supported across the full path, not from the largest value a broker accepts.',
          },
          {
            type: 'table',
            headers: ['Limit', 'Recommended starting point', 'Why'],
            rows: [
              ['Payload', '1 MiB', 'Large payloads amplify copies, latency, and retry cost.'],
              ['Headers', '16 KiB', 'Prevents trace, error, or custom metadata growth.'],
              ['Error details', '1–4 KiB', 'Avoids stack traces becoming a second payload.'],
              ['Request timeout', '2–10 seconds', 'Keeps caller pressure bounded during dependency failure.'],
            ],
          },
        ],
      },
      {
        id: 'transport-configuration',
        title: 'Transport configuration',
        blocks: [
          {
            type: 'paragraph',
            text: 'Each adapter has its own configuration object. Keep broker-specific settings there instead of forcing them into one generic map. This makes unsupported combinations visible to analysis, tests, and code review.',
          },
          {
            type: 'bullets',
            items: [
              'NATS: servers, authentication, JetStream stream and consumer settings.',
              'RabbitMQ: URI, exchanges, prefetch, confirms, retry topology, reconnect, and TLS.',
              'Kafka: bootstrap servers, group ID, security properties, timeouts, and native library location.',
              'PostgreSQL: schema names, lease durations, polling, attempt limits, and worker identity.',
            ],
          },
        ],
      },
      {
        id: 'environment-variables',
        title: 'Environment variables',
        blocks: [
          {
            type: 'paragraph',
            text: 'Read secrets and endpoints from your deployment environment, then construct typed configuration at startup. Validate everything before serving traffic.',
          },
          {
            type: 'code',
            language: 'dart',
            code: `final natsUrl = Platform.environment['NATS_URL'];
if (natsUrl == null) {
  throw StateError('NATS_URL is required');
}

final config = NatsMessagingConfig(
  servers: [Uri.parse(natsUrl)],
);`,
          },
          {
            type: 'note',
            tone: 'danger',
            title: 'Never log connection URIs blindly',
            text: 'Broker and database URIs may contain usernames, passwords, tokens, hosts, and tenant identifiers. Redact them before writing structured logs.',
          },
        ],
      },
    ],
  },
];
