import type { DocPage } from '@/lib/docs-types';

export const integrationDocs: DocPage[] = [
  {
    slug: 'postgresql',
    title: 'PostgreSQL',
    description:
      'Transactional outbox, inbox leases, and persistent idempotency for multi-replica services.',
    category: 'Integrations',
    order: 1,
    sections: [
      {
        id: 'package',
        title: 'What podbus_postgres provides',
        blocks: [
          {
            type: 'bullets',
            items: [
              'PostgresOutbox for writing outgoing messages inside an existing database transaction.',
              'PostgresOutboxRelay for leased, multi-replica publication.',
              'PostgresInbox for consumer-side acquisition, completion, failure, and lease recovery.',
              'PostgresIdempotencyStore for shared duplicate protection.',
              'PostgresMessagingSchema for installing or managing the supporting tables.',
            ],
          },
        ],
      },
      {
        id: 'connection-pool',
        title: 'Connection pool',
        blocks: [
          {
            type: 'code',
            language: 'dart',
            code: `import 'package:postgres/postgres.dart';
import 'package:podbus_postgres/podbus_postgres.dart';

final pool = Pool<void>.withUrl(
  Uri.parse(Platform.environment['DATABASE_URL']!),
);

final schema = PostgresMessagingSchema();
await pool.run(schema.install);`,
          },
          {
            type: 'paragraph',
            text: 'Install schema changes through the same migration process used by the rest of the application. Calling install during startup is convenient for development, but production migrations should be reviewed, ordered, and applied before new application code starts.',
          },
        ],
      },
      {
        id: 'outbox-relay',
        title: 'Outbox relay lifecycle',
        blocks: [
          {
            type: 'paragraph',
            text: 'Each relay replica needs a unique worker ID. Pending records are leased so another replica can recover them after the lease expires. The relay publishes, waits for the transport result, and then updates the database record.',
          },
          {
            type: 'code',
            language: 'dart',
            code: `final relay = PostgresOutboxRelay(
  outbox: PostgresOutbox(pool),
  bus: bus,
  workerId: 'orders-api-\${Platform.localHostname}',
);

Future<void> relayLoop() async {
  while (!shutdownRequested) {
    final count = await relay.runOnce();
    if (count == 0) {
      await Future<void>.delayed(const Duration(milliseconds: 250));
    }
  }
}`,
          },
        ],
      },
      {
        id: 'pool-sizing',
        title: 'Pool sizing',
        blocks: [
          {
            type: 'paragraph',
            text: 'Outbox relays, inbox handlers, HTTP requests, and background workers all compete for database connections. Size the application pool from measured concurrency and database capacity. Do not set worker concurrency higher than the number of connections and downstream slots available to the handler path.',
          },
          {
            type: 'table',
            headers: ['Workload', 'Connection behavior'],
            rows: [
              ['Outbox relay', 'Short transactions for leasing and completion; broker wait should stay outside long locks.'],
              ['Inbox processing', 'Acquire and completion transactions; business work may need its own transaction.'],
              ['Idempotency claim', 'Small conflict-heavy operation requiring a useful unique index.'],
              ['Cleanup', 'Batch deletion or archival can create I/O spikes if unbounded.'],
            ],
          },
        ],
      },
      {
        id: 'maintenance',
        title: 'Maintenance and retention',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Index pending status, lease expiry, and creation time for relay scans.',
              'Archive or delete published rows in bounded batches.',
              'Keep completed inbox and idempotency rows for the full duplicate window.',
              'Monitor table and index bloat.',
              'Back up reliability tables with the same point-in-time policy as business tables.',
              'Test restore consistency between business state, outbox, and inbox records.',
            ],
          },
        ],
      },
    ],
  },
  {
    slug: 'serverpod',
    title: 'Serverpod',
    description:
      'Start and stop transports safely and open a fresh Serverpod session for each message.',
    category: 'Integrations',
    order: 2,
    sections: [
      {
        id: 'purpose',
        title: 'Why the integration exists',
        blocks: [
          {
            type: 'paragraph',
            text: 'Serverpod sessions carry database access, logging, authentication context, and lifecycle. A long-lived session should not be shared across unrelated message handlers. podbus_serverpod creates and closes one session per delivery.',
          },
        ],
      },
      {
        id: 'startup',
        title: 'Configure lifecycle',
        blocks: [
          {
            type: 'code',
            language: 'dart',
            code: `final messaging = ServerpodMessaging<Session>(
  bus: bus,
  queue: queue,
  sessionFactory: () => pod.createSession(enableLogging: true),
  closeSession: (session) => session.close(),
);

await messaging.start();`,
          },
          {
            type: 'paragraph',
            text: 'Startup is rollback-safe. If the queue cannot connect or a worker registration fails, already-opened resources are closed instead of leaving a partially started service.',
          },
        ],
      },
      {
        id: 'handlers',
        title: 'Message and job handlers',
        blocks: [
          {
            type: 'code',
            language: 'dart',
            code: `await messaging.worker<Map<String, Object?>>(
  'jobs.lead-score',
  durableName: 'lead-score-v1',
  concurrency: 4,
  handler: (session, context, payload) async {
    final leadId = payload['leadId']! as int;
    final lead = await Lead.db.findById(session, leadId);
    if (lead == null) {
      throw StateError('Lead $leadId does not exist');
    }

    lead.score = calculateScore(lead);
    await Lead.db.updateRow(session, lead);
  },
);`,
          },
        ],
      },
      {
        id: 'transactions',
        title: 'Transactions inside handlers',
        blocks: [
          {
            type: 'paragraph',
            text: 'Use a database transaction when inbox completion, business state, and new outbox records must commit together. The broker acknowledgement should occur only after the transaction completes successfully.',
          },
          {
            type: 'note',
            tone: 'warning',
            title: 'Do not keep transactions open across slow remote calls',
            text: 'Long database transactions increase lock time and connection pressure. For remote side effects, use idempotency and a state machine instead of holding a transaction while waiting on the network.',
          },
        ],
      },
      {
        id: 'shutdown',
        title: 'Shutdown',
        blocks: [
          {
            type: 'paragraph',
            text: 'Stop new Serverpod traffic, mark readiness unhealthy, stop new broker deliveries, drain active sessions, close worker registrations, close transports, and only then terminate the process.',
          },
          {
            type: 'code',
            language: 'dart',
            code: `await messaging.stop(
  timeout: const Duration(seconds: 30),
);`,
          },
        ],
      },
    ],
  },
  {
    slug: 'observability',
    title: 'Observability',
    description:
      'Trace message flow, export bounded metrics, redact logs, and aggregate health state.',
    category: 'Integrations',
    order: 3,
    sections: [
      {
        id: 'package',
        title: 'Observability package',
        blocks: [
          {
            type: 'paragraph',
            text: 'podbus_observability is framework-neutral. It provides instrumentation decorators and data structures rather than forcing one HTTP server, metrics exporter, or tracing backend into every service.',
          },
          {
            type: 'bullets',
            items: [
              'W3C traceparent and tracestate propagation.',
              'Producer, consumer, request, and worker span records.',
              'Bounded-cardinality Prometheus text output.',
              'Structured JSON log records with redaction.',
              'Readiness and liveness aggregation across components.',
            ],
          },
        ],
      },
      {
        id: 'tracing',
        title: 'Tracing',
        blocks: [
          {
            type: 'code',
            language: 'dart',
            code: `final tracer = PodBusTracer(
  export: (span) => spanExporter.add(span),
);

final tracedBus = InstrumentedMessageBus(
  delegate: bus,
  tracer: tracer,
  transport: 'nats',
);`,
          },
          {
            type: 'paragraph',
            text: 'The decorator injects trace context into outgoing headers and extracts it for incoming handlers. A consumer span should be linked to the producing trace while remaining honest about asynchronous timing.',
          },
        ],
      },
      {
        id: 'metrics',
        title: 'Prometheus metrics',
        blocks: [
          {
            type: 'code',
            language: 'dart',
            code: `final metrics = PrometheusRegistry(
  maxSeries: 2000,
  allowedLabelKeys: {
    'transport',
    'topic',
    'status',
    'unit',
  },
);

final config = MessagingConfig(
  metricHook: metrics.hook,
);`,
          },
          {
            type: 'note',
            tone: 'danger',
            title: 'Protect metric cardinality',
            text: 'Never use message IDs, customer IDs, email addresses, arbitrary error strings, or unbounded routing values as labels. High-cardinality metrics can exhaust the monitoring system during the incident you are trying to observe.',
          },
        ],
      },
      {
        id: 'logging',
        title: 'Structured logging',
        blocks: [
          {
            type: 'code',
            language: 'dart',
            code: `final logs = JsonMessagingLogSink(
  serviceName: 'billing-worker',
  write: stdout.writeln,
  includeStackTraces: false,
  maxValueCharacters: 2048,
);`,
          },
          {
            type: 'paragraph',
            text: 'The default sensitive-key list redacts credentials, tokens, payloads, email addresses, and phone fields. Extend the list for domain-specific data. Keep stack traces in protected diagnostics rather than public log streams.',
          },
        ],
      },
      {
        id: 'health',
        title: 'Health probes',
        blocks: [
          {
            type: 'code',
            language: 'dart',
            code: `final probe = PodBusHealthProbe(
  checks: {
    'events': bus.healthCheck,
    'jobs': queue.healthCheck,
    'database': databaseHealthCheck,
  },
);

final ready = await probe.readiness();
response.statusCode = ready.statusCode;
response.write(ready.toJson());`,
          },
          {
            type: 'paragraph',
            text: 'Readiness answers whether the instance should receive new work. Liveness answers whether the process should be restarted. A degraded broker can make readiness fail without necessarily making liveness fail.',
          },
        ],
      },
      {
        id: 'baseline-metrics',
        title: 'Baseline metrics',
        blocks: [
          {
            type: 'table',
            headers: ['Metric', 'Why it matters'],
            rows: [
              ['published_total', 'Producer throughput and unexpected drops.'],
              ['received_total', 'Ingress rate.'],
              ['processed_total', 'Successful handler completion.'],
              ['failed_total', 'Failure rate by bounded route and transport labels.'],
              ['retried_total', 'Dependency instability and poison workloads.'],
              ['dead_lettered_total', 'Terminal processing failures.'],
              ['duplicate_total', 'Idempotency pressure and redelivery behavior.'],
              ['handler_duration', 'Capacity and acknowledgement-window tuning.'],
              ['consumer_lag', 'Backlog and retention risk.'],
              ['active_handlers', 'Current concurrency.'],
              ['reconnect_total', 'Broker or network instability.'],
              ['unroutable_total', 'RabbitMQ routing defects.'],
            ],
          },
        ],
      },
    ],
  },
];
