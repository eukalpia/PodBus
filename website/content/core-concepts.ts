import type { DocPage } from '@/lib/docs-types';

export const coreConceptDocs: DocPage[] = [
  {
    slug: 'message-bus',
    title: 'Message bus',
    description:
      'Publish events, subscribe with bounded concurrency, and use request/reply when supported.',
    category: 'Core concepts',
    order: 1,
    sections: [
      {
        id: 'contract',
        title: 'The MessageBus contract',
        blocks: [
          {
            type: 'paragraph',
            text: 'MessageBus is the event-oriented PodBus contract. It covers connection lifecycle, publish, subscribe, request/reply, capability discovery, and health checks.',
          },
          {
            type: 'code',
            language: 'dart',
            code: `abstract interface class MessageBus {
  MessagingCapabilities get capabilities;

  Future<void> connect();
  Future<void> publish<T>(
    String subject,
    T payload, {
    MessageHeaders? headers,
  });
  Future<Subscription> subscribe<T>(
    String subject, {
    String? queueGroup,
    int concurrency = 1,
    required MessageHandler<T> handler,
  });
  Future<TResponse> request<TRequest, TResponse>(
    String subject,
    TRequest payload, {
    MessageHeaders? headers,
    Duration? timeout,
  });
  Future<HealthCheckResult> healthCheck();
  Future<void> close({Duration? timeout});
}`,
          },
          {
            type: 'note',
            tone: 'info',
            title: 'The interface is broader than every transport',
            text: 'Call `capabilities.requireAll(...)` when your application depends on request/reply, queue groups, graceful shutdown, or another optional behavior.',
          },
        ],
      },
      {
        id: 'subjects',
        title: 'Subjects and routing keys',
        blocks: [
          {
            type: 'paragraph',
            text: 'The first string passed to publish or subscribe is called a subject in the core API. The adapter maps it to the transport concept: a NATS subject, RabbitMQ routing key, or Kafka topic.',
          },
          {
            type: 'bullets',
            items: [
              'Use stable domain names such as `billing.invoice-issued` or `jobs.email.welcome`.',
              'Avoid embedding customer IDs or other high-cardinality data in the subject unless partitioning requires it.',
              'Version the payload schema, not the routing name, for compatible changes.',
              'Create a new message type when the meaning changes materially.',
            ],
          },
        ],
      },
      {
        id: 'subscriptions',
        title: 'Subscriptions and queue groups',
        blocks: [
          {
            type: 'paragraph',
            text: 'A subscription without a queue group receives its own copy of every matching event. Subscribers sharing a queue group compete for each event. The exact routing implementation remains transport-specific.',
          },
          {
            type: 'code',
            language: 'dart',
            code: `final subscription = await bus.subscribe<OrderCreated>(
  'orders.created',
  queueGroup: 'analytics-workers',
  concurrency: 16,
  handler: (context, event) async {
    await analytics.record(event);
  },
);`,
          },
          {
            type: 'note',
            tone: 'warning',
            title: 'Concurrency is a pressure valve',
            text: 'Higher concurrency is useful only when downstream systems can absorb it. Set database pools, HTTP connection pools, and broker prefetch limits together.',
          },
        ],
      },
      {
        id: 'request-reply',
        title: 'Request/reply',
        blocks: [
          {
            type: 'paragraph',
            text: 'Request/reply sends a message and waits for one response until a timeout. It is useful for low-latency service coordination but still creates temporal coupling between services.',
          },
          {
            type: 'code',
            language: 'dart',
            code: `final result = await bus.request<PriceRequest, PriceResponse>(
  'pricing.quote',
  PriceRequest(productId: 'sku-42'),
  timeout: const Duration(seconds: 2),
);`,
          },
          {
            type: 'bullets',
            items: [
              'Use explicit deadlines.',
              'Propagate correlation and trace context.',
              'Keep handlers idempotent where retries are possible.',
              'Prefer durable workflows for operations that must survive caller failure.',
            ],
          },
        ],
      },
    ],
  },
  {
    slug: 'durable-jobs',
    title: 'Durable jobs',
    description:
      'Run acknowledged background work with retry, dead-letter, and idempotency policies.',
    category: 'Core concepts',
    order: 2,
    sections: [
      {
        id: 'contract',
        title: 'The DurableJobQueue contract',
        blocks: [
          {
            type: 'paragraph',
            text: 'DurableJobQueue is for work that should survive process restarts and broker redelivery. Enqueue creates work; worker registers a handler with durable identity, concurrency, retry, and dead-letter policies.',
          },
          {
            type: 'code',
            language: 'dart',
            code: `final worker = await queue.worker<GenerateInvoice>(
  'jobs.invoice.generate',
  durableName: 'invoice-generator-v1',
  concurrency: 4,
  retryPolicy: RetryPolicy(
    maxAttempts: 7,
    initialDelay: const Duration(milliseconds: 250),
    maxDelay: const Duration(seconds: 30),
    jitter: 0.2,
  ),
  deadLetterPolicy: const DeadLetterPolicy(
    enabled: true,
    destination: 'jobs.invoice.generate.dead',
  ),
  handler: (context, job) async {
    await invoiceService.generate(job.invoiceId);
  },
);`,
          },
        ],
      },
      {
        id: 'acknowledgement',
        title: 'Acknowledgement lifecycle',
        blocks: [
          {
            type: 'steps',
            items: [
              {
                title: 'Receive',
                description: 'The adapter receives a broker delivery and constructs a job context.',
              },
              {
                title: 'Decode and validate',
                description: 'Wire metadata, payload limits, message type, and schema version are checked.',
              },
              {
                title: 'Run the handler',
                description: 'The user handler performs the business side effect.',
              },
              {
                title: 'Classify failure',
                description: 'A failure is marked transient, permanent, malformed, authorization-related, or infrastructure-related.',
              },
              {
                title: 'Ack, retry, or dead-letter',
                description: 'The source delivery is finalized only after the selected outcome is confirmed.',
              },
            ],
          },
        ],
      },
      {
        id: 'durable-names',
        title: 'Durable consumer names',
        blocks: [
          {
            type: 'paragraph',
            text: 'A durable name is persistent processing state. Renaming it may create a new consumer and replay retained messages. Treat it like a database migration, not a cosmetic identifier.',
          },
          {
            type: 'bullets',
            items: [
              'Include the logical handler name.',
              'Add a version only when replay or state separation is intentional.',
              'Keep the name stable across replicas.',
              'Document the expected start position and retention window.',
            ],
          },
          {
            type: 'code',
            language: 'text',
            code: `Good: billing-invoice-projector-v1
Good: welcome-email-sender
Risky: worker-\${DateTime.now()}
Risky: pod-\${Platform.localHostname}`,
          },
        ],
      },
      {
        id: 'scheduling',
        title: 'Delayed execution',
        blocks: [
          {
            type: 'paragraph',
            text: 'The enqueue API can express a future run time, but support depends on the adapter. RabbitMQ uses broker retry topology for delayed redelivery; JetStream uses acknowledgement delay; the in-memory queue uses process-local timers. Check capabilities before depending on scheduling.',
          },
          {
            type: 'note',
            tone: 'warning',
            title: 'Do not use process-local delay for critical work',
            text: 'A timer in one process disappears when the process restarts. Use a broker or database-backed scheduling mechanism when delayed work must survive failure.',
          },
        ],
      },
    ],
  },
  {
    slug: 'capabilities',
    title: 'Capabilities',
    description:
      'Require transport behavior explicitly and fail before serving traffic.',
    category: 'Core concepts',
    order: 3,
    sections: [
      {
        id: 'why-capabilities',
        title: 'Why capabilities exist',
        blocks: [
          {
            type: 'paragraph',
            text: 'A shared interface is convenient, but a false promise is dangerous. Capabilities let the application state what it requires and let the adapter report what it actually implements.',
          },
          {
            type: 'code',
            language: 'dart',
            code: `queue.capabilities.requireAll({
  MessagingCapability.durableJobs,
  MessagingCapability.deadLettering,
  MessagingCapability.gracefulShutdown,
});`,
          },
        ],
      },
      {
        id: 'startup-validation',
        title: 'Validate during startup',
        blocks: [
          {
            type: 'paragraph',
            text: 'Run capability checks after constructing configuration and before opening network traffic. A missing feature should prevent readiness, not become a runtime UnsupportedError after the first customer message.',
          },
          {
            type: 'code',
            language: 'dart',
            code: `Future<void> startApplication() async {
  final queue = buildQueue();

  queue.capabilities.requireAll({
    MessagingCapability.durableJobs,
    MessagingCapability.idempotentPublish,
  });

  await queue.connect();
  await registerWorkers(queue);
  await httpServer.start();
}`,
          },
        ],
      },
      {
        id: 'capability-matrix',
        title: 'Common capability groups',
        blocks: [
          {
            type: 'table',
            headers: ['Group', 'Examples'],
            rows: [
              ['Eventing', 'publish/subscribe, queue groups, request/reply'],
              ['Durability', 'durable jobs, manual acknowledgement, dead-letter handling'],
              ['Timing', 'delayed delivery, broker-managed retry'],
              ['Safety', 'publisher confirms, idempotency hooks, graceful shutdown'],
              ['Operations', 'health checks, lag or pending state, reconnect behavior'],
            ],
          },
        ],
      },
      {
        id: 'degradation',
        title: 'Graceful degradation',
        blocks: [
          {
            type: 'paragraph',
            text: 'Some applications can run with reduced behavior. For example, an optional analytics subscriber may be disabled when the selected transport lacks queue groups. Make this branch explicit and observable.',
          },
          {
            type: 'note',
            tone: 'danger',
            title: 'Never silently downgrade durability',
            text: 'Do not replace durable jobs with process-local work because a transport lacks persistence. Fail startup or disable the feature with a clear health signal.',
          },
        ],
      },
    ],
  },
  {
    slug: 'headers-and-context',
    title: 'Headers and context',
    description:
      'Carry correlation, causation, tracing, idempotency, and controlled custom metadata.',
    category: 'Core concepts',
    order: 4,
    sections: [
      {
        id: 'message-headers',
        title: 'MessageHeaders',
        blocks: [
          {
            type: 'paragraph',
            text: 'MessageHeaders contains transport-neutral metadata used across the wire protocol. Standard fields are kept separate from custom headers to avoid collisions and simplify observability.',
          },
          {
            type: 'code',
            language: 'dart',
            code: `final headers = MessageHeaders(
  correlationId: requestId,
  causationId: commandId,
  traceId: currentTraceId,
  custom: {
    'tenant-region': 'eu-west',
    'producer': 'billing-api',
  },
);`,
          },
        ],
      },
      {
        id: 'correlation-causation',
        title: 'Correlation and causation',
        blocks: [
          {
            type: 'table',
            headers: ['Field', 'Purpose', 'Lifetime'],
            rows: [
              ['correlationId', 'Groups messages that belong to one business flow.', 'Usually the entire workflow.'],
              ['causationId', 'Identifies the message or command that caused this message.', 'One edge in the message graph.'],
              ['traceId', 'Connects broker processing to distributed tracing.', 'One trace, subject to sampling.'],
              ['idempotencyKey', 'Protects a side effect or publish from duplicate execution.', 'Business-operation specific.'],
            ],
          },
        ],
      },
      {
        id: 'handler-context',
        title: 'Handler context',
        blocks: [
          {
            type: 'paragraph',
            text: 'Message handlers receive context beside the decoded payload. The context contains headers and delivery metadata such as attempt count. Durable workers also expose the retry boundary needed to make decisions and emit useful logs.',
          },
          {
            type: 'code',
            language: 'dart',
            code: `handler: (context, job) async {
  logger.info('processing job', {
    'correlationId': context.headers.correlationId,
    'attempt': context.attempt,
    'maxAttempts': context.maxAttempts,
  });

  await process(job);
}`, 
          },
        ],
      },
      {
        id: 'header-safety',
        title: 'Header safety',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Enforce a maximum encoded header size.',
              'Do not place full payloads, tokens, cookies, or personal data in headers.',
              'Reserve PodBus wire keys for the protocol.',
              'Keep custom keys stable and lowercase where possible.',
              'Truncate diagnostic values before dead-letter publication.',
            ],
          },
        ],
      },
    ],
  },
  {
    slug: 'typed-codecs',
    title: 'Typed codecs',
    description:
      'Separate Dart runtime types from stable wire names and schema versions.',
    category: 'Core concepts',
    order: 5,
    sections: [
      {
        id: 'registry',
        title: 'Codec registry',
        blocks: [
          {
            type: 'paragraph',
            text: 'The codec registry maps a Dart type to a stable message type name, schema version, encoder, and decoder. The stable name belongs to the wire contract; the Dart class name can change without changing the protocol.',
          },
          {
            type: 'code',
            language: 'dart',
            code: `final codecs = MessageCodecRegistry()
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
  );`,
          },
        ],
      },
      {
        id: 'wire-identity',
        title: 'Wire identity',
        blocks: [
          {
            type: 'paragraph',
            text: 'A message is identified by more than its route. The envelope carries a stable message type and schema version so a consumer can validate the payload before entering business logic.',
          },
          {
            type: 'code',
            language: 'json',
            code: `{
  "specVersion": 1,
  "id": "01J...",
  "messageType": "crm.lead-created",
  "schemaVersion": 2,
  "timestamp": "2026-07-11T06:00:00Z",
  "contentType": "application/json",
  "payload": {
    "leadId": 42
  }
}`,
          },
        ],
      },
      {
        id: 'schema-evolution',
        title: 'Schema evolution',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Add optional fields with defaults for backward-compatible changes.',
              'Keep decoders for every version still present in broker retention or replay archives.',
              'Upcast old payloads before business logic.',
              'Reject unknown future versions and send them to a controlled dead-letter path.',
              'Create a new message type when meaning changes, even if the JSON shape looks similar.',
            ],
          },
        ],
      },
      {
        id: 'untyped-json',
        title: 'When untyped JSON is acceptable',
        blocks: [
          {
            type: 'paragraph',
            text: 'Map<String, Object?> is practical for prototypes, integration boundaries, and generic relay services. For long-lived domain events, a registered type makes compatibility, tests, and ownership clearer.',
          },
          {
            type: 'note',
            tone: 'warning',
            title: 'A route is not a schema',
            text: 'Subscribing to `orders.created` does not tell the decoder which historical payload versions may arrive. Keep schema identity in the envelope.',
          },
        ],
      },
    ],
  },
  {
    slug: 'retries-and-failures',
    title: 'Retries and failures',
    description:
      'Classify failures before retrying and bound every retry loop.',
    category: 'Core concepts',
    order: 6,
    sections: [
      {
        id: 'classification',
        title: 'Failure classification',
        blocks: [
          {
            type: 'table',
            headers: ['Class', 'Typical examples', 'Default action'],
            rows: [
              ['Transient', 'Timeout, temporary network failure, unavailable dependency.', 'Retry with backoff.'],
              ['Rate limited', 'HTTP 429, broker quota, downstream throttle.', 'Retry after a bounded delay.'],
              ['Infrastructure', 'Broker disconnect, database failover.', 'Retry or pause consumption.'],
              ['Permanent', 'Business rule rejection, deleted entity, invalid state.', 'Dead-letter or mark failed.'],
              ['Malformed', 'Invalid JSON, missing required field, unsupported schema.', 'Dead-letter without repeated retry.'],
              ['Authorization', 'Expired credentials, forbidden operation.', 'Alert and stop or dead-letter according to policy.'],
            ],
          },
        ],
      },
      {
        id: 'retry-policy',
        title: 'Retry policy',
        blocks: [
          {
            type: 'code',
            language: 'dart',
            code: `final policy = RetryPolicy(
  maxAttempts: 6,
  initialDelay: const Duration(milliseconds: 250),
  maxDelay: const Duration(seconds: 30),
  backoffMultiplier: 2,
  jitter: 0.2,
);`,
          },
          {
            type: 'paragraph',
            text: 'Exponential backoff reduces synchronized pressure on a failing dependency. Jitter prevents every worker from retrying at the same instant. maxAttempts and maxDelay keep the failure finite.',
          },
        ],
      },
      {
        id: 'retry-boundaries',
        title: 'Retry boundaries',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Retry one message, not the entire consumer process.',
              'Do not retry malformed payloads.',
              'Do not retry a business rejection unless state may legitimately change.',
              'Include a maximum attempt count and an operational time limit.',
              'Confirm retry publication before acknowledging the source delivery.',
              'Protect downstream systems from replay storms.',
            ],
          },
        ],
      },
      {
        id: 'application-retries',
        title: 'Application retries inside a handler',
        blocks: [
          {
            type: 'paragraph',
            text: 'A short local retry can be useful for a highly transient call, but nested retry loops multiply attempts and hide total latency. Prefer one clearly owned retry policy. If the broker owns redelivery, keep handler-level retries small and observable.',
          },
          {
            type: 'note',
            tone: 'danger',
            title: 'Count the multiplication',
            text: 'Five HTTP retries inside a handler with five broker attempts can execute the downstream operation twenty-five times. Idempotency and budgets must cover the combined behavior.',
          },
        ],
      },
    ],
  },
  {
    slug: 'dead-letters',
    title: 'Dead letters',
    description:
      'Quarantine messages that cannot be processed and replay them deliberately.',
    category: 'Core concepts',
    order: 7,
    sections: [
      {
        id: 'purpose',
        title: 'What a dead-letter destination is for',
        blocks: [
          {
            type: 'paragraph',
            text: 'A dead-letter destination preserves the fact that work could not be processed after policy was exhausted or a permanent failure was identified. It is an operational queue for diagnosis and controlled recovery, not a second retry loop.',
          },
        ],
      },
      {
        id: 'policy',
        title: 'Dead-letter policy',
        blocks: [
          {
            type: 'code',
            language: 'dart',
            code: `const deadLetters = DeadLetterPolicy(
  enabled: true,
  destination: 'jobs.invoice.generate.dead',
  includeErrorDetails: true,
  includeOriginalPayload: false,
);`,
          },
          {
            type: 'paragraph',
            text: 'Exclude the original payload by default when messages may contain personal, financial, health, authentication, or other sensitive data. Error details should be truncated and redacted.',
          },
        ],
      },
      {
        id: 'metadata',
        title: 'Useful dead-letter metadata',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Original message ID and message type.',
              'Schema version and source route.',
              'Correlation and causation identifiers.',
              'Final attempt count.',
              'Failure class and redacted error summary.',
              'First-seen and dead-lettered timestamps.',
              'Consumer or worker identity.',
            ],
          },
        ],
      },
      {
        id: 'replay',
        title: 'Replay workflow',
        blocks: [
          {
            type: 'steps',
            items: [
              {
                title: 'Understand the failure',
                description: 'Separate code defects, bad data, missing dependencies, and incompatible schemas.',
              },
              {
                title: 'Fix or transform',
                description: 'Deploy the fix or produce a corrected payload with an auditable transformation.',
              },
              {
                title: 'Replay a canary',
                description: 'Send a small number of messages and verify side effects and deduplication.',
              },
              {
                title: 'Rate-limit the replay',
                description: 'Protect the broker and downstream systems from a sudden backlog burst.',
              },
              {
                title: 'Record provenance',
                description: 'Track operator, reason, source message ID, replay ID, and timestamp.',
              },
            ],
          },
          {
            type: 'note',
            tone: 'warning',
            title: 'A DLQ is not a backup',
            text: 'Retention may be short and payloads may be intentionally omitted. Preserve business-critical source data in the authoritative system of record.',
          },
        ],
      },
    ],
  },
];
