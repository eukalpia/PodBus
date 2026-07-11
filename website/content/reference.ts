import type { DocPage } from '@/lib/docs-types';

export const referenceDocs: DocPage[] = [
  {
    slug: 'api-reference',
    title: 'API reference',
    description:
      'A compact reference for the primary PodBus contracts and reliability helpers.',
    category: 'Reference',
    order: 1,
    sections: [
      {
        id: 'message-bus',
        title: 'MessageBus',
        blocks: [
          {
            type: 'table',
            headers: ['Member', 'Purpose'],
            rows: [
              ['capabilities', 'Reports supported transport behavior.'],
              ['connect()', 'Opens transport resources and validates required topology.'],
              ['publish(subject, payload)', 'Publishes one event.'],
              ['subscribe(subject, handler)', 'Registers an event consumer.'],
              ['request(subject, payload)', 'Sends one request and waits for a response where supported.'],
              ['healthCheck()', 'Returns healthy, degraded, or unhealthy transport state.'],
              ['close(timeout)', 'Stops new work and closes resources within a deadline.'],
            ],
          },
        ],
      },
      {
        id: 'durable-job-queue',
        title: 'DurableJobQueue',
        blocks: [
          {
            type: 'table',
            headers: ['Member', 'Purpose'],
            rows: [
              ['capabilities', 'Reports durable, retry, dead-letter, and shutdown behavior.'],
              ['connect()', 'Opens the broker connection and required durable infrastructure.'],
              ['enqueue(topic, payload)', 'Creates a durable job.'],
              ['worker(topic, handler)', 'Registers a durable handler with policy and concurrency.'],
              ['healthCheck()', 'Returns worker transport health.'],
              ['close(timeout)', 'Drains active handlers and closes the queue.'],
            ],
          },
        ],
      },
      {
        id: 'headers',
        title: 'MessageHeaders',
        blocks: [
          {
            type: 'table',
            headers: ['Field', 'Use'],
            rows: [
              ['messageId', 'Stable identity for one physical message.'],
              ['correlationId', 'Groups a business flow across messages and services.'],
              ['causationId', 'Identifies the command or message that caused this message.'],
              ['traceId', 'Links transport processing to tracing.'],
              ['idempotencyKey', 'Protects one logical operation from duplicates.'],
              ['custom', 'Bounded application metadata outside reserved wire keys.'],
            ],
          },
        ],
      },
      {
        id: 'policies',
        title: 'Policies',
        blocks: [
          {
            type: 'table',
            headers: ['Type', 'Important fields'],
            rows: [
              ['RetryPolicy', 'maxAttempts, initialDelay, maxDelay, multiplier, jitter'],
              ['DeadLetterPolicy', 'enabled, destination, includeErrorDetails, includeOriginalPayload'],
              ['MessagingConfig', 'codec registry, payload/header limits, timeouts, hooks, failure classifier'],
            ],
          },
        ],
      },
      {
        id: 'subscriptions-workers',
        title: 'Subscription and Worker handles',
        blocks: [
          {
            type: 'paragraph',
            text: 'Registration returns a handle representing the active subscription or worker. Keep the handle for lifecycle management. Closing the handle stops new deliveries and participates in graceful shutdown.',
          },
          {
            type: 'code',
            language: 'dart',
            code: `final subscription = await bus.subscribe<Event>(
  'domain.event',
  handler: handleEvent,
);

final worker = await queue.worker<Job>(
  'jobs.domain.work',
  durableName: 'domain-worker-v1',
  handler: handleJob,
);

await subscription.close();
await worker.close();`,
          },
        ],
      },
      {
        id: 'postgres-types',
        title: 'PostgreSQL reliability types',
        blocks: [
          {
            type: 'table',
            headers: ['Type', 'Responsibility'],
            rows: [
              ['PostgresMessagingSchema', 'Creates or manages outbox, inbox, and idempotency tables.'],
              ['PostgresOutbox', 'Writes and leases outgoing messages.'],
              ['PostgresOutboxRelay', 'Publishes leased outbox records and records outcomes.'],
              ['PostgresInbox', 'Acquires, completes, fails, and recovers incoming message leases.'],
              ['PostgresIdempotencyStore', 'Claims and releases shared operation keys.'],
            ],
          },
        ],
      },
      {
        id: 'observability-types',
        title: 'Observability types',
        blocks: [
          {
            type: 'table',
            headers: ['Type', 'Responsibility'],
            rows: [
              ['W3cTraceContext', 'Parses, creates, injects, and extracts trace context.'],
              ['PodBusTracer', 'Creates span records and exports completed spans.'],
              ['InstrumentedMessageBus', 'Adds tracing around publish, request, and consume.'],
              ['InstrumentedDurableJobQueue', 'Adds tracing around enqueue and job processing.'],
              ['PrometheusRegistry', 'Records bounded metric series and renders text format.'],
              ['JsonMessagingLogSink', 'Writes structured, redacted JSON logs.'],
              ['PodBusHealthProbe', 'Aggregates component checks into readiness and liveness responses.'],
            ],
          },
        ],
      },
    ],
  },
  {
    slug: 'configuration-reference',
    title: 'Configuration reference',
    description:
      'Shared and transport-specific configuration with production-oriented guidance.',
    category: 'Reference',
    order: 2,
    sections: [
      {
        id: 'messaging-config',
        title: 'MessagingConfig',
        blocks: [
          {
            type: 'table',
            headers: ['Option', 'Purpose', 'Guidance'],
            rows: [
              ['codecRegistry', 'Maps Dart types to stable wire contracts.', 'Register all long-lived domain events explicitly.'],
              ['maxPayloadBytes', 'Rejects oversized encoded payloads.', 'Start at 1 MiB or less.'],
              ['maxHeaderBytes', 'Rejects oversized wire metadata.', 'Start at 16 KiB or less.'],
              ['requestTimeout', 'Default request/reply deadline.', 'Derive from the caller budget.'],
              ['retryPolicy', 'Default retry behavior.', 'Override for workloads with different recovery profiles.'],
              ['metricHook', 'Receives structured metric events.', 'Keep labels bounded.'],
              ['logHook', 'Receives structured log events.', 'Redact before writing.'],
              ['failureClassifier', 'Maps errors to retry or terminal classes.', 'Keep deterministic and test it.'],
            ],
          },
        ],
      },
      {
        id: 'nats-config',
        title: 'NATS configuration',
        blocks: [
          {
            type: 'table',
            headers: ['Option group', 'Examples'],
            rows: [
              ['Connection', 'server URIs, connection name, reconnect behavior, timeouts'],
              ['Authentication', 'token, credentials, NKey, JWT, TLS context'],
              ['JetStream stream', 'stream name, subjects, retention, replicas, storage'],
              ['Consumer', 'durable name, ack wait, max deliver, max ack pending, heartbeat'],
            ],
          },
          {
            type: 'note',
            tone: 'warning',
            title: 'Avoid conflicting authentication methods',
            text: 'Configure one intended authentication path. Do not combine token and username/password credentials accidentally.',
          },
        ],
      },
      {
        id: 'rabbit-config',
        title: 'RabbitMQ configuration',
        blocks: [
          {
            type: 'table',
            headers: ['Option', 'Purpose'],
            rows: [
              ['uri', 'amqp or amqps endpoint including virtual host.'],
              ['exchange', 'Primary topic exchange.'],
              ['deadLetterExchange', 'Destination exchange for terminal failures.'],
              ['retryExchange', 'Exchange used by broker-native retry queues.'],
              ['prefetchCount', 'Maximum unacknowledged deliveries per consumer channel.'],
              ['publisherConfirmTimeout', 'Upper bound for broker confirmation.'],
              ['mandatoryPublish', 'Returns unroutable messages instead of dropping them.'],
              ['useBrokerRetryQueues', 'Uses TTL/DLX retry topology.'],
              ['maxConnectionAttempts', 'Bounded connection retry during startup.'],
              ['reconnectWaitTime', 'Delay between connection attempts.'],
              ['tlsContext', 'Certificate trust and client identity.'],
            ],
          },
        ],
      },
      {
        id: 'kafka-config',
        title: 'Kafka configuration',
        blocks: [
          {
            type: 'table',
            headers: ['Area', 'Guidance'],
            rows: [
              ['Bootstrap servers', 'Provide multiple broker endpoints where available.'],
              ['Group ID', 'Stable identity for one logical consumer group.'],
              ['Client ID', 'Stable service identity for broker observability.'],
              ['Request timeout', 'Bounds assignment and broker operations.'],
              ['Security protocol', 'Use TLS and the cluster-required SASL mechanism.'],
              ['Native library path', 'Set PODBUS_LIBRDKAFKA_PATH when discovery is insufficient.'],
              ['Producer properties', 'Configure acknowledgements, queue limits, batching, and compression deliberately.'],
            ],
          },
        ],
      },
      {
        id: 'postgres-config',
        title: 'PostgreSQL configuration',
        blocks: [
          {
            type: 'table',
            headers: ['Area', 'Guidance'],
            rows: [
              ['Table names', 'Use a controlled schema and migration ownership.'],
              ['Lease duration', 'Longer than normal processing, shorter than unacceptable recovery delay.'],
              ['Batch size', 'Small enough to avoid long locks and broker bursts.'],
              ['Worker ID', 'Unique per relay or consumer replica.'],
              ['Maximum attempts', 'Finite, with terminal failure visibility.'],
              ['Retention', 'Aligned with replay and duplicate windows.'],
            ],
          },
        ],
      },
    ],
  },
  {
    slug: 'capability-matrix',
    title: 'Capability matrix',
    description:
      'Compare the behavior currently exposed by each PodBus adapter.',
    category: 'Reference',
    order: 3,
    sections: [
      {
        id: 'matrix',
        title: 'Current adapter capabilities',
        blocks: [
          {
            type: 'table',
            headers: ['Capability', 'In-memory', 'NATS Core', 'JetStream', 'RabbitMQ', 'Kafka'],
            rows: [
              ['Publish / subscribe', 'Yes', 'Yes', 'No', 'Yes', 'Yes'],
              ['Queue groups', 'Yes', 'Yes', 'No', 'Yes', 'Consumer groups'],
              ['Request / reply', 'Yes', 'Yes', 'No', 'No', 'No'],
              ['Durable jobs', 'Test only', 'No', 'Yes', 'Yes', 'Yes'],
              ['Delayed retry', 'Process local', 'No', 'NAK delay', 'TTL / DLX', 'Application policy'],
              ['Dead-letter handling', 'Yes', 'No', 'Yes', 'Yes', 'Yes'],
              ['Manual ack / commit', 'No', 'No', 'Yes', 'Yes', 'Yes'],
              ['Typed codecs', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes'],
              ['Persistent idempotency', 'External', 'External', 'External', 'External', 'External'],
              ['Transactional outbox', 'PostgreSQL helper', 'PostgreSQL helper', 'PostgreSQL helper', 'PostgreSQL helper', 'PostgreSQL helper'],
              ['Maturity', 'Development', 'Reference', 'Reference', 'Beta', 'Experimental'],
            ],
          },
          {
            type: 'note',
            tone: 'info',
            title: 'Runtime capabilities win',
            text: 'Documentation can become stale. The adapter capability set used by your deployed revision is the runtime source of truth.',
          },
        ],
      },
      {
        id: 'selection',
        title: 'Selection guide',
        blocks: [
          {
            type: 'table',
            headers: ['Need', 'First transport to evaluate'],
            rows: [
              ['Low-latency events and request/reply', 'NATS Core'],
              ['Durable work with simple subject routing', 'NATS JetStream'],
              ['Queue semantics, topic routing, and confirms', 'RabbitMQ'],
              ['Retained event log and partition replay', 'Kafka, with experimental caveat'],
              ['Database mutation plus eventual publish', 'Any transport plus PostgreSQL outbox'],
              ['Cross-replica duplicate suppression', 'Any transport plus PostgreSQL inbox/idempotency'],
            ],
          },
        ],
      },
    ],
  },
  {
    slug: 'faq',
    title: 'FAQ',
    description:
      'Common questions about broker choice, guarantees, Serverpod, performance, and release status.',
    category: 'Reference',
    order: 4,
    sections: [
      {
        id: 'broker',
        title: 'Is PodBus a broker?',
        blocks: [
          {
            type: 'paragraph',
            text: 'No. PodBus is a Dart library that provides contracts and adapters for existing brokers plus PostgreSQL reliability patterns. You still deploy, secure, monitor, and operate the underlying infrastructure.',
          },
        ],
      },
      {
        id: 'exactly-once',
        title: 'Does PodBus provide exactly-once delivery?',
        blocks: [
          {
            type: 'paragraph',
            text: 'No blanket exactly-once claim is made. Durable workers use at-least-once delivery. Use outbox, inbox, persistent idempotency, database constraints, and provider idempotency keys to make business side effects safe.',
          },
        ],
      },
      {
        id: 'transport-choice',
        title: 'Which transport should I choose?',
        blocks: [
          {
            type: 'paragraph',
            text: 'Start from the failure and replay model, not from a benchmark. NATS Core is for transient low-latency messaging, JetStream for durable work, RabbitMQ for queue-oriented routing and confirms, and Kafka for retained partitioned logs when the experimental adapter is acceptable.',
          },
        ],
      },
      {
        id: 'serverpod-only',
        title: 'Is PodBus only for Serverpod?',
        blocks: [
          {
            type: 'paragraph',
            text: 'No. The core and broker packages work in plain Dart services. podbus_serverpod is an optional integration that manages Serverpod sessions and lifecycle.',
          },
        ],
      },
      {
        id: 'flutter',
        title: 'Should a Flutter client connect directly to PodBus brokers?',
        blocks: [
          {
            type: 'paragraph',
            text: 'Usually no. Mobile clients should call an authenticated application API or realtime gateway. Exposing broker credentials, subjects, and topology to untrusted devices creates security, compatibility, and operational problems.',
          },
        ],
      },
      {
        id: 'large-payloads',
        title: 'Can I publish large files?',
        blocks: [
          {
            type: 'paragraph',
            text: 'Store large files in object storage and publish a reference, checksum, content type, size, and authorization context. Large broker payloads increase copying, memory pressure, retry cost, retention cost, and dead-letter risk.',
          },
        ],
      },
      {
        id: 'ordering',
        title: 'How do I guarantee ordering?',
        blocks: [
          {
            type: 'paragraph',
            text: 'Define the smallest ordering scope that matters, route that scope consistently, keep concurrency compatible with it, and account for retries. Global ordering is expensive and is not provided by the common API.',
          },
        ],
      },
      {
        id: 'performance',
        title: 'Which broker is fastest?',
        blocks: [
          {
            type: 'paragraph',
            text: 'The answer changes with persistence, replication, acknowledgements, batching, payload size, hardware, topology, and handler work. Benchmark the exact guarantee and workload you plan to deploy, then evaluate latency distribution and failure recovery, not only peak throughput.',
          },
        ],
      },
      {
        id: 'pubdev',
        title: 'Are the packages on pub.dev?',
        blocks: [
          {
            type: 'paragraph',
            text: 'Not yet. Install from Git and pin a tested revision. Publication should happen after package metadata, API boundaries, compatibility policy, and release automation are stable.',
          },
        ],
      },
      {
        id: 'production-ready',
        title: 'Is PodBus production-ready?',
        blocks: [
          {
            type: 'paragraph',
            text: 'The current release is alpha. NATS is the reference path, RabbitMQ is suitable for controlled production evaluation, and Kafka is experimental. A stable 1.0 requires sustained fault and soak evidence, compatibility commitments, and independent production deployments.',
          },
        ],
      },
    ],
  },
  {
    slug: 'roadmap',
    title: 'Roadmap',
    description:
      'The work required to move from alpha foundations to a stable public API.',
    category: 'Reference',
    order: 5,
    sections: [
      {
        id: 'alpha',
        title: 'Alpha priorities',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Keep CI, integration, compatibility, and security gates green.',
              'Expand compatibility fixtures for wire protocol and schemas.',
              'Add more broker restart, duplicate, slow-consumer, and network-interruption tests.',
              'Polish package-level documentation and examples.',
              'Validate NATS and RabbitMQ in controlled real deployments.',
            ],
          },
        ],
      },
      {
        id: 'beta',
        title: 'Beta priorities',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Freeze the main public contracts behind a compatibility policy.',
              'Publish packages with signed provenance and reproducible release metadata.',
              'Complete Kafka delivery reports, rebalance coordination, and multi-partition commit tracking.',
              'Add controlled DLQ inspection and replay tooling.',
              'Run sustained soak tests across current and previous broker versions.',
              'Publish migration guides for every public API change.',
            ],
          },
        ],
      },
      {
        id: 'stable',
        title: '1.0 criteria',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Stable, documented public API and wire compatibility policy.',
              'No known critical delivery-loss path in supported configurations.',
              'Verified upgrade and rollback paths.',
              'Fault, load, and long-running soak evidence.',
              'Multiple independent production deployments.',
              'Security response, release, and deprecation processes in regular use.',
            ],
          },
          {
            type: 'note',
            tone: 'info',
            title: 'What 1.0 will not mean',
            text: 'It will not mean one broker model, universal ordering, or exactly-once side effects across arbitrary external systems. Stability means the guarantees are clear, tested, and compatible.',
          },
        ],
      },
    ],
  },
];
