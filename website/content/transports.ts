import type { DocPage } from '@/lib/docs-types';

export const transportDocs: DocPage[] = [
  {
    slug: 'nats-core',
    title: 'NATS Core',
    description:
      'Low-latency publish/subscribe, queue groups, and request/reply for transient messaging.',
    category: 'Transports',
    order: 1,
    badge: 'Reference',
    sections: [
      {
        id: 'when-to-use',
        title: 'When to use NATS Core',
        blocks: [
          {
            type: 'paragraph',
            text: 'Use NATS Core when low latency and simple subject-based routing matter more than broker persistence. It is a good fit for live notifications, service coordination, cache invalidation, ephemeral fan-out, and request/reply.',
          },
          {
            type: 'note',
            tone: 'warning',
            title: 'Core messages are not durable jobs',
            text: 'If no subscriber is available, a Core NATS message is not retained for later delivery. Use JetStream or another durable transport when work must survive downtime.',
          },
        ],
      },
      {
        id: 'configuration',
        title: 'Configuration',
        blocks: [
          {
            type: 'code',
            language: 'dart',
            code: `final config = NatsMessagingConfig(
  servers: [
    Uri.parse('nats://nats-a.internal:4222'),
    Uri.parse('nats://nats-b.internal:4222'),
  ],
);

final bus = NatsMessageBus(
  config: config,
  messagingConfig: messagingConfig,
);`,
          },
          {
            type: 'bullets',
            items: [
              'Configure more than one server in clustered environments.',
              'Use TLS with a scoped token or username/password credentials. NKey and JWT authentication are not exposed by the current adapter.',
              'Give each service only the subjects it must publish or subscribe to.',
              'Set request timeouts from the caller budget, not an arbitrary global default.',
            ],
          },
        ],
      },
      {
        id: 'queue-groups',
        title: 'Queue groups',
        blocks: [
          {
            type: 'paragraph',
            text: 'Subscribers in the same NATS queue group share messages. This is useful for horizontal scaling, but the message remains transient: queue groups distribute delivery; they do not persist it.',
          },
          {
            type: 'code',
            language: 'dart',
            code: `await bus.subscribe<Map<String, Object?>>(
  'inventory.changed',
  queueGroup: 'search-indexers',
  concurrency: 32,
  handler: (context, event) async {
    await searchIndex.update(event);
  },
);`,
          },
        ],
      },
      {
        id: 'request-reply',
        title: 'Request/reply',
        blocks: [
          {
            type: 'paragraph',
            text: 'NATS request/reply is efficient for small synchronous exchanges. Keep the deadline short and make failure behavior explicit. A request that must survive caller or responder downtime should be modeled as durable work instead.',
          },
          {
            type: 'code',
            language: 'dart',
            code: `final reply = await bus.request<AvailabilityQuery, AvailabilityReply>(
  'inventory.availability',
  AvailabilityQuery(productId: 'sku-42'),
  timeout: const Duration(milliseconds: 750),
);`,
          },
        ],
      },
      {
        id: 'operations',
        title: 'Operational notes',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Monitor reconnects and slow-consumer signals.',
              'Drain subscriptions before closing the client.',
              'Avoid unbounded handler concurrency.',
              'Keep subjects predictable and permission-friendly.',
              'Do not use Core NATS for a business command that cannot be lost.',
            ],
          },
        ],
      },
    ],
  },
  {
    slug: 'jetstream',
    title: 'NATS JetStream',
    description:
      'Durable workers with retained messages, acknowledgement, redelivery, and consumer state.',
    category: 'Transports',
    order: 2,
    badge: 'Reference',
    sections: [
      {
        id: 'when-to-use',
        title: 'When to use JetStream',
        blocks: [
          {
            type: 'paragraph',
            text: 'Use JetStream for background work and event consumption that must survive process restarts or temporary consumer downtime. Streams retain messages; durable consumers track processing state.',
          },
        ],
      },
      {
        id: 'stream-configuration',
        title: 'Stream configuration',
        blocks: [
          {
            type: 'code',
            language: 'dart',
            code: `final queue = NatsJetStreamJobQueue(
  config: NatsMessagingConfig(
    servers: [Uri.parse('nats://localhost:4222')],
    jetStream: const NatsJetStreamConfig(
      enabled: true,
      streamName: 'PODBUS_JOBS',
      subjects: ['jobs.>'],
    ),
  ),
);`,
          },
          {
            type: 'paragraph',
            text: 'The stream name and subject set are persistent broker state. Keep their definitions in version control and review retention, storage, replica count, maximum age, maximum bytes, and discard policy with the same care as a database schema. The alpha adapter exposes stream retention and replica options, but advanced consumer tuning remains a production validation item.',
          },
        ],
      },
      {
        id: 'consumer-tuning',
        title: 'Consumer settings to validate',
        blocks: [
          {
            type: 'table',
            headers: ['Setting', 'Purpose'],
            rows: [
              ['ackWait', 'How long a handler may run before the broker considers delivery unacknowledged.'],
              ['maxDeliver', 'Broker-level upper bound on redelivery attempts.'],
              ['maxAckPending', 'Backpressure limit for outstanding unacknowledged messages.'],
              ['durable name', 'Persistent consumer identity and cursor.'],
              ['heartbeat / in-progress', 'Keeps long-running handlers from appearing abandoned.'],
              ['replicas', 'How many JetStream nodes hold stream data.'],
            ],
          },
          {
            type: 'note',
            tone: 'warning',
            title: 'ackWait must exceed realistic handler time',
            text: 'When handlers regularly exceed the acknowledgement window, the same message may run concurrently on another worker. The current alpha API does not expose every JetStream consumer knob; validate broker defaults and redesign long jobs into smaller steps where necessary.',
          },
        ],
      },
      {
        id: 'retry',
        title: 'Retry and termination',
        blocks: [
          {
            type: 'paragraph',
            text: 'Transient failures can be negatively acknowledged with delay. Permanent or malformed failures should be terminated and optionally published to a dead-letter subject. Retry or dead-letter publication must complete before the source delivery is finalized.',
          },
          {
            type: 'code',
            language: 'dart',
            code: `await queue.worker<GenerateReport>(
  'jobs.report.generate',
  durableName: 'report-generator-v1',
  retryPolicy: RetryPolicy(
    maxAttempts: 5,
    initialDelay: const Duration(seconds: 1),
    maxDelay: const Duration(minutes: 1),
    jitter: 0.25,
  ),
  deadLetterPolicy: const DeadLetterPolicy(
    enabled: true,
    destination: 'jobs.report.generate.dead',
  ),
  handler: (context, job) => reports.generate(job),
);`,
          },
        ],
      },
      {
        id: 'production-checklist',
        title: 'Production checklist',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Use file-backed streams for durable work.',
              'Set replicas according to the broker failure domain.',
              'Set retention larger than the longest expected consumer outage.',
              'Use stable durable names.',
              'Test broker restart, reconnect, duplicate delivery, and slow consumers.',
              'Monitor pending messages, redeliveries, consumer errors, and stream storage.',
            ],
          },
        ],
      },
    ],
  },
  {
    slug: 'rabbitmq',
    title: 'RabbitMQ',
    description:
      'Queue-oriented messaging with topic routing, publisher confirms, TTL/DLX retries, and dead letters.',
    category: 'Transports',
    order: 3,
    badge: 'Beta',
    sections: [
      {
        id: 'when-to-use',
        title: 'When to use RabbitMQ',
        blocks: [
          {
            type: 'paragraph',
            text: 'RabbitMQ is a strong fit for queue-oriented work, topic routing, competing consumers, publisher confirmation, and broker-managed retry topology. It is often the simplest choice when each job should be processed by one worker and routing rules are central to the design.',
          },
        ],
      },
      {
        id: 'configuration',
        title: 'Configuration',
        blocks: [
          {
            type: 'code',
            language: 'dart',
            code: `final config = RabbitMqMessagingConfig(
  uri: Uri.parse('amqps://podbus@rabbit.internal:5671/app'),
  exchange: 'podbus.events',
  deadLetterExchange: 'podbus.dead',
  retryExchange: 'podbus.retry',
  prefetchCount: 32,
  mandatoryPublish: true,
  publisherConfirmTimeout: const Duration(seconds: 5),
  useBrokerRetryQueues: true,
);`,
          },
          {
            type: 'bullets',
            items: [
              'Use `amqps://` and certificate validation in production.',
              'Separate users and virtual hosts by environment or trust boundary.',
              'Set prefetch near the tested concurrency and handler memory budget.',
              'Keep publisher confirms and mandatory routing enabled.',
              'Use distinct publisher and consumer channels.',
            ],
          },
        ],
      },
      {
        id: 'publisher-confirms',
        title: 'Publisher confirms and returned messages',
        blocks: [
          {
            type: 'paragraph',
            text: 'A successful write to the client socket is not proof that the broker accepted or routed the message. The adapter waits for publisher confirmation. Mandatory routing causes unroutable messages to be returned instead of silently discarded.',
          },
          {
            type: 'note',
            tone: 'danger',
            title: 'Confirm does not mean consumed',
            text: 'A publisher confirm proves broker acceptance according to RabbitMQ semantics. It does not prove that a consumer processed the message or that the business side effect succeeded.',
          },
        ],
      },
      {
        id: 'retry-topology',
        title: 'TTL and dead-letter retry topology',
        blocks: [
          {
            type: 'paragraph',
            text: 'Broker-native retry uses a retry exchange and queues with message TTL plus dead-letter routing back to the source exchange. This avoids sleeping inside a consumer while holding capacity.',
          },
          {
            type: 'code',
            language: 'text',
            code: `source exchange
      │
      ▼
worker queue ── failure ──► retry exchange
                               │
                               ▼
                         TTL retry queue
                               │ expires
                               ▼
                         source exchange`,
          },
          {
            type: 'paragraph',
            text: 'A small fixed set of retry buckets is easier to operate than creating one queue for every arbitrary delay. Choose buckets that match the application recovery profile.',
          },
        ],
      },
      {
        id: 'temporary-subscriptions',
        title: 'Temporary subscriptions',
        blocks: [
          {
            type: 'paragraph',
            text: 'A subscription without a durable queue group should use an exclusive, auto-delete, non-durable queue. A durable worker queue should have a stable name and durable topology.',
          },
        ],
      },
      {
        id: 'operations',
        title: 'Operational checklist',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Monitor connection and channel closure separately.',
              'Re-declare exchanges, queues, bindings, and consumers after reconnect.',
              'Use quorum queues where the workload and cluster design justify them.',
              'Alert on unroutable messages, nacks, queue depth, redelivery, and disk alarms.',
              'Test broker restart, credential rotation, certificate failure, and topology mismatch.',
              'Do not acknowledge the source before retry or dead-letter publication is confirmed.',
            ],
          },
        ],
      },
    ],
  },
  {
    slug: 'kafka',
    title: 'Kafka',
    description:
      'Experimental append-only event-log integration through native librdkafka bindings.',
    category: 'Transports',
    order: 4,
    badge: 'Experimental',
    sections: [
      {
        id: 'status',
        title: 'Current status',
        blocks: [
          {
            type: 'note',
            tone: 'warning',
            title: 'Experimental adapter',
            text: 'Use the Kafka adapter only when you can test and accept its behavior. NATS JetStream or RabbitMQ should be preferred for production durable jobs until Kafka delivery reports, rebalance behavior, and crash semantics are proven across supported librdkafka versions.',
          },
          {
            type: 'paragraph',
            text: 'PodBus owns the minimal native librdkafka FFI contract instead of importing private APIs from another Dart package. This reduces one source of breakage but does not remove the operational complexity of Kafka.',
          },
        ],
      },
      {
        id: 'when-to-use',
        title: 'When Kafka fits',
        blocks: [
          {
            type: 'bullets',
            items: [
              'A retained append-only stream is part of the architecture.',
              'Consumers need independent offsets and replay.',
              'Ordering by partition key is meaningful.',
              'The organization already operates Kafka well.',
              'High sustained throughput matters more than queue-style routing.',
            ],
          },
        ],
      },
      {
        id: 'native-library',
        title: 'Native librdkafka dependency',
        blocks: [
          {
            type: 'paragraph',
            text: 'The adapter loads librdkafka at runtime. The environment variable `PODBUS_LIBRDKAFKA_PATH` can point to an explicit shared library when it is not available through the platform default search path.',
          },
          {
            type: 'code',
            language: 'bash',
            code: `export PODBUS_LIBRDKAFKA_PATH=/usr/lib/x86_64-linux-gnu/librdkafka.so.1`,
          },
        ],
      },
      {
        id: 'consumer-groups',
        title: 'Consumer groups and assignment',
        blocks: [
          {
            type: 'paragraph',
            text: 'A consumer group divides partitions among active members. Subscription is not ready until the broker assigns at least one partition or the configured timeout expires. Rebalances can revoke and reassign partitions while the service is running.',
          },
          {
            type: 'bullets',
            items: [
              'Do not report readiness before assignment when the service requires active consumption.',
              'Do not process more records concurrently than the commit strategy can track safely.',
              'Treat partition revocation as a shutdown boundary for affected work.',
              'Use a stable key when per-aggregate ordering matters.',
            ],
          },
        ],
      },
      {
        id: 'commit-ordering',
        title: 'Commit ordering',
        blocks: [
          {
            type: 'paragraph',
            text: 'The source offset must not be committed before the handler outcome and any retry or dead-letter publication are confirmed. Otherwise the source record can be lost while the replacement record never became durable.',
          },
          {
            type: 'code',
            language: 'text',
            code: `poll record
   │
   ├─ handler success ───────────────► commit source offset
   │
   ├─ retryable failure ─► publish retry ─► confirm ─► commit
   │
   └─ terminal failure ──► publish DLQ ───► flush ───► commit`,
          },
        ],
      },
      {
        id: 'production-gaps',
        title: 'Production gaps to validate',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Per-message delivery reports and error classification.',
              'Cooperative rebalance callbacks and in-flight handler coordination.',
              'Multiple-partition concurrency with exact offset tracking.',
              'Retry-topic and dead-letter-topic lifecycle.',
              'SASL/SCRAM, TLS, certificate validation, and ACL examples.',
              'Crash tests before and after side effects, publication, flush, and commit.',
              'Backpressure when the producer queue is full.',
              'Compatibility across supported librdkafka and Kafka broker versions.',
            ],
          },
        ],
      },
    ],
  },
];
