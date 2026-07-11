import type { DocPage } from '@/lib/docs-types';

export const operationsDocs: DocPage[] = [
  {
    slug: 'production-deployment',
    title: 'Production deployment',
    description:
      'A deployment baseline for transport security, capacity, readiness, retention, and rollback.',
    category: 'Operations',
    order: 1,
    sections: [
      {
        id: 'preconditions',
        title: 'Before deployment',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Pin all PodBus packages to one tested revision.',
              'Run format, analysis, unit, integration, compatibility, and security workflows.',
              'Verify transport capabilities during startup.',
              'Apply database migrations before code that depends on them.',
              'Install dashboards and alerts before enabling traffic.',
              'Document rollback, replay, and dead-letter procedures.',
            ],
          },
        ],
      },
      {
        id: 'security',
        title: 'Transport security',
        blocks: [
          {
            type: 'table',
            headers: ['System', 'Production baseline'],
            rows: [
              ['NATS', 'TLS plus scoped credentials, NKey, or JWT accounts.'],
              ['RabbitMQ', 'amqps, certificate verification, dedicated users, and isolated vhosts.'],
              ['Kafka', 'TLS plus SASL/SCRAM or the cluster-required mechanism and narrow ACLs.'],
              ['PostgreSQL', 'TLS with certificate verification and a least-privilege database role.'],
            ],
          },
          {
            type: 'note',
            tone: 'danger',
            title: 'Do not bypass certificate validation',
            text: 'Accepting every certificate turns encrypted transport into unauthenticated transport. Install the correct trust chain instead.',
          },
        ],
      },
      {
        id: 'capacity',
        title: 'Capacity planning',
        blocks: [
          {
            type: 'paragraph',
            text: 'Size the system from peak ingress, average and tail handler duration, retry bursts, consumer downtime, payload size, replication cost, and downstream capacity. A broker benchmark by itself is not an application capacity plan.',
          },
          {
            type: 'code',
            language: 'text',
            code: `required concurrency ≈ peak messages per second × handler seconds

example:
  800 msg/s × 0.050 s = 40 concurrent handlers

then add measured headroom, not an arbitrary multiplier`,
          },
          {
            type: 'bullets',
            items: [
              'Keep consumer concurrency within database and HTTP pool capacity.',
              'Set broker pending limits and application queues to create backpressure.',
              'Model retry traffic as additional ingress.',
              'Keep enough retention for the longest credible outage and recovery time.',
              'Measure p95 and p99 handler duration, not only averages.',
            ],
          },
        ],
      },
      {
        id: 'configuration',
        title: 'Configuration baseline',
        blocks: [
          {
            type: 'table',
            headers: ['Concern', 'Baseline'],
            rows: [
              ['Payload size', 'Start at 1 MiB or lower.'],
              ['Header size', 'Start at 16 KiB or lower.'],
              ['Request timeout', 'Derived from the caller deadline.'],
              ['Shutdown timeout', 'Shorter than the platform termination grace period.'],
              ['Retry attempts', 'Finite, with exponential backoff and jitter.'],
              ['Dead-letter retention', 'Long enough for diagnosis, bounded by data policy.'],
              ['Durable names', 'Stable and versioned only when replay is intentional.'],
              ['Secrets', 'Environment, Vault, or platform secret store; never source control.'],
            ],
          },
        ],
      },
      {
        id: 'release-gate',
        title: 'Release gate',
        blocks: [
          {
            type: 'steps',
            items: [
              {
                title: 'Build and verify',
                description: 'Static analysis, tests, coverage, broker integration, security, and package metadata all pass.',
              },
              {
                title: 'Exercise failure paths',
                description: 'Restart brokers, interrupt networks, force duplicates, and test slow consumers.',
              },
              {
                title: 'Deploy a canary',
                description: 'Run one replica with real dependencies and bounded traffic.',
              },
              {
                title: 'Inspect the message path',
                description: 'Check lag, retries, dead letters, outbox age, logs, and traces.',
              },
              {
                title: 'Scale gradually',
                description: 'Increase replicas while observing downstream saturation and rebalance behavior.',
              },
            ],
          },
        ],
      },
    ],
  },
  {
    slug: 'health-checks',
    title: 'Health checks',
    description:
      'Separate liveness, readiness, degradation, and component diagnostics.',
    category: 'Operations',
    order: 2,
    sections: [
      {
        id: 'states',
        title: 'Health states',
        blocks: [
          {
            type: 'table',
            headers: ['State', 'Meaning', 'Typical platform action'],
            rows: [
              ['healthy', 'The component can perform its required work.', 'Keep instance ready.'],
              ['degraded', 'Work may continue with reduced capacity or delayed behavior.', 'Usually remove from readiness or alert, depending on role.'],
              ['unhealthy', 'The component cannot perform required work.', 'Fail readiness; restart only when liveness also fails.'],
            ],
          },
        ],
      },
      {
        id: 'readiness',
        title: 'Readiness',
        blocks: [
          {
            type: 'paragraph',
            text: 'Readiness answers whether this instance should receive new traffic or broker work. It should fail during startup, drain, missing capability, broker unavailability, or a critical database dependency outage.',
          },
          {
            type: 'bullets',
            items: [
              'Required broker connection is established.',
              'Worker registrations and durable consumers are active.',
              'Required partition assignment is present.',
              'Database connectivity is available for inbox or outbox work.',
              'The instance is not draining.',
            ],
          },
        ],
      },
      {
        id: 'liveness',
        title: 'Liveness',
        blocks: [
          {
            type: 'paragraph',
            text: 'Liveness answers whether restarting the process is likely to help. A temporary broker outage should not cause every replica to restart continuously. Deadlocked event loops, unrecoverable internal state, or a failed supervisor may justify liveness failure.',
          },
          {
            type: 'note',
            tone: 'warning',
            title: 'Do not turn dependency outages into restart storms',
            text: 'Fail readiness when an external dependency is unavailable. Keep liveness healthy while the client can reconnect and the process remains responsive.',
          },
        ],
      },
      {
        id: 'details',
        title: 'Useful component details',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Connection state and last successful reconnect.',
              'Last successful publish and consume timestamps.',
              'Last redacted worker error type.',
              'Active and pending handler counts.',
              'Consumer lag or pending messages.',
              'Oldest outbox record age.',
              'Dead-letter growth and unroutable publication count.',
            ],
          },
          {
            type: 'note',
            tone: 'info',
            title: 'Health endpoints are not log dumps',
            text: 'Expose bounded, non-sensitive state. Do not return full stack traces, payloads, credentials, or internal network topology.',
          },
        ],
      },
    ],
  },
  {
    slug: 'graceful-shutdown',
    title: 'Graceful shutdown',
    description:
      'Stop new work, drain active handlers, flush publication, and exit within a deadline.',
    category: 'Operations',
    order: 3,
    sections: [
      {
        id: 'sequence',
        title: 'Shutdown sequence',
        blocks: [
          {
            type: 'steps',
            items: [
              {
                title: 'Enter draining state',
                description: 'Reject new application traffic and make readiness fail.',
              },
              {
                title: 'Stop consumption',
                description: 'Cancel subscriptions or stop polling so no new handlers begin.',
              },
              {
                title: 'Wait for active handlers',
                description: 'Allow successful handlers to acknowledge normally.',
              },
              {
                title: 'Handle the deadline',
                description: 'Unfinished broker deliveries remain unacknowledged or are requeued according to adapter semantics.',
              },
              {
                title: 'Flush and close',
                description: 'Flush producers, close channels and clients, then close database pools.',
              },
            ],
          },
        ],
      },
      {
        id: 'signals',
        title: 'Process signals',
        blocks: [
          {
            type: 'code',
            language: 'dart',
            code: `final signals = StreamGroup.merge([
  ProcessSignal.sigterm.watch(),
  ProcessSignal.sigint.watch(),
]);

await for (final _ in signals) {
  readiness.disable();
  await messaging.close(timeout: const Duration(seconds: 30));
  await database.close();
  break;
}`,
          },
        ],
      },
      {
        id: 'deadline',
        title: 'Deadline design',
        blocks: [
          {
            type: 'paragraph',
            text: 'The platform termination grace period must be longer than the application drain timeout. Leave time for pre-stop delay, process signal delivery, client flush, and final cleanup.',
          },
          {
            type: 'code',
            language: 'text',
            code: `Kubernetes terminationGracePeriodSeconds: 45
preStop propagation delay:                 5
application drain timeout:                30
remaining hard-exit buffer:               10`,
          },
        ],
      },
      {
        id: 'long-running-jobs',
        title: 'Long-running jobs',
        blocks: [
          {
            type: 'paragraph',
            text: 'A job that routinely exceeds the deployment shutdown window is difficult to operate. Split it into checkpoints, persist progress, or use a workflow engine. For JetStream, report in-progress status within ackWait. For other transports, ensure redelivery does not run the same unsafe side effect concurrently.',
          },
        ],
      },
      {
        id: 'tests',
        title: 'Shutdown tests',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Terminate while handlers are idle.',
              'Terminate while one handler succeeds before the deadline.',
              'Terminate while one handler exceeds the deadline.',
              'Terminate while retry publication is pending.',
              'Terminate while dead-letter publication is pending.',
              'Send a second termination signal and verify hard-exit behavior.',
            ],
          },
        ],
      },
    ],
  },
  {
    slug: 'testing',
    title: 'Testing',
    description:
      'Separate unit, integration, fault, compatibility, load, and soak testing.',
    category: 'Operations',
    order: 4,
    sections: [
      {
        id: 'layers',
        title: 'Testing layers',
        blocks: [
          {
            type: 'table',
            headers: ['Layer', 'Purpose'],
            rows: [
              ['Unit', 'Policies, codecs, limits, context propagation, state machines.'],
              ['Adapter contract', 'Behavior against fakes or deterministic protocol boundaries.'],
              ['Broker integration', 'Real publish, consume, acknowledgement, retry, and dead-letter behavior.'],
              ['Fault injection', 'Restart, network interruption, timeout, duplicate, and crash windows.'],
              ['Compatibility', 'Supported Dart, broker, native library, and schema versions.'],
              ['Load', 'Capacity, latency distribution, memory, and backpressure.'],
              ['Soak', 'Leaks, reconnect drift, queue growth, and long-running stability.'],
            ],
          },
        ],
      },
      {
        id: 'commands',
        title: 'Local commands',
        blocks: [
          {
            type: 'code',
            language: 'bash',
            code: `dart pub get
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
  --exclude-tags=integration`,
          },
          {
            type: 'code',
            language: 'bash',
            code: `docker compose -f docker-compose.integration.yaml up -d \
  nats rabbitmq kafka postgres

PODBUS_RUN_INTEGRATION_TESTS=true dart test \
  packages/podbus_nats/test \
  packages/podbus_rabbitmq/test \
  packages/podbus_kafka/test \
  packages/podbus_postgres/test \
  --tags=integration`,
          },
        ],
      },
      {
        id: 'fault-cases',
        title: 'Minimum fault cases',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Broker restart while publishing and consuming.',
              'Network loss and recovery.',
              'Handler crash before and after a side effect.',
              'Crash before and after acknowledgement or commit.',
              'Malformed JSON and unsupported schema version.',
              'Oversized payload and headers.',
              'Unroutable RabbitMQ publish.',
              'Kafka rebalance during in-flight processing.',
              'Duplicate delivery across replicas.',
              'Slow consumer and exhausted downstream pool.',
              'Shutdown with active handlers.',
              'Outbox relay death after publish but before database completion.',
            ],
          },
        ],
      },
      {
        id: 'benchmarking',
        title: 'Benchmarking rules',
        blocks: [
          {
            type: 'paragraph',
            text: 'A messages-per-second number without broker configuration, payload size, persistence mode, replication, acknowledgements, hardware, handler behavior, and latency distribution is not useful. Use benchmarks to compare changes in one documented environment and to size a real workload.',
          },
          {
            type: 'bullets',
            items: [
              'Report p50, p95, and p99 latency.',
              'Separate producer acceptance from end-to-end processing.',
              'State persistence, replication, acknowledgement, and batching settings.',
              'Measure memory, CPU, network, disk, and broker backlog.',
              'Run a long enough soak to expose leaks and reconnect drift.',
            ],
          },
        ],
      },
    ],
  },
  {
    slug: 'github-pages',
    title: 'GitHub Pages deployment',
    description:
      'Build the Next.js documentation site as a static export and publish it with GitHub Actions.',
    category: 'Operations',
    order: 5,
    sections: [
      {
        id: 'architecture',
        title: 'Static export architecture',
        blocks: [
          {
            type: 'paragraph',
            text: 'The documentation site uses the Next.js App Router with `output: export`. Every documentation route is generated during the build and written to the `out` directory. GitHub Pages serves the resulting HTML, CSS, JavaScript, and image files without a Node.js server.',
          },
          {
            type: 'code',
            language: 'javascript',
            filename: 'website/next.config.mjs',
            code: `const nextConfig = {
  output: 'export',
  trailingSlash: true,
  basePath: process.env.GITHUB_ACTIONS === 'true' ? '/PodBus' : '',
  assetPrefix: process.env.GITHUB_ACTIONS === 'true' ? '/PodBus' : '',
  images: { unoptimized: true },
};`,
          },
        ],
      },
      {
        id: 'workflow',
        title: 'Deployment workflow',
        blocks: [
          {
            type: 'paragraph',
            text: 'The Pages workflow installs exact application dependencies, builds the site, uploads the `out` directory as the Pages artifact, and deploys through the protected `github-pages` environment.',
          },
          {
            type: 'code',
            language: 'yaml',
            filename: '.github/workflows/pages.yml',
            code: `- uses: actions/setup-node@<pinned-sha>
  with:
    node-version: 24
    cache: npm
    cache-dependency-path: website/package-lock.json

- working-directory: website
  run: npm ci

- working-directory: website
  run: npm run build

- uses: actions/upload-pages-artifact@<pinned-sha>
  with:
    path: website/out`,
          },
        ],
      },
      {
        id: 'enable-pages',
        title: 'Enable Pages once',
        blocks: [
          {
            type: 'steps',
            items: [
              {
                title: 'Open repository settings',
                description: 'Go to Settings → Pages.',
              },
              {
                title: 'Choose GitHub Actions',
                description: 'Set the build and deployment source to GitHub Actions.',
              },
              {
                title: 'Run the Pages workflow',
                description: 'Push a website change or dispatch the workflow manually.',
              },
              {
                title: 'Verify the environment',
                description: 'The deployment should create or update the `github-pages` environment and publish its URL.',
              },
            ],
          },
          {
            type: 'note',
            tone: 'info',
            title: 'Why the screenshot showed 404',
            text: 'Repository files alone do not activate GitHub Pages. The repository must use GitHub Actions as its Pages source and complete one successful deployment.',
          },
        ],
      },
      {
        id: 'local-development',
        title: 'Local development',
        blocks: [
          {
            type: 'code',
            language: 'bash',
            code: `cd website
npm install
npm run dev`,
          },
          {
            type: 'paragraph',
            text: 'Local development runs without the `/PodBus` base path. The GitHub Actions environment enables the repository base path during the production export.',
          },
        ],
      },
    ],
  },
  {
    slug: 'incident-response',
    title: 'Incident response',
    description:
      'Respond to broker outages, lag, dead-letter growth, duplicate storms, and poison messages.',
    category: 'Operations',
    order: 6,
    sections: [
      {
        id: 'first-principles',
        title: 'First principles',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Stop irreversible data loss and uncontrolled amplification first.',
              'Preserve broker, application, and database evidence before restarting everything.',
              'Reduce or pause producers before increasing consumers during downstream failure.',
              'Do not replay dead letters until the failure class is understood.',
              'Keep idempotency enabled during recovery.',
            ],
          },
        ],
      },
      {
        id: 'broker-unavailable',
        title: 'Broker unavailable',
        blocks: [
          {
            type: 'steps',
            items: [
              {
                title: 'Freeze change',
                description: 'Stop deployments and configuration changes while diagnosing.',
              },
              {
                title: 'Locate the boundary',
                description: 'Check application, DNS, network, certificates, credentials, broker quorum, and disk alarms.',
              },
              {
                title: 'Preserve durable ingress',
                description: 'Keep business transactions writing to the outbox; do not bypass it with direct publishes.',
              },
              {
                title: 'Restore connectivity',
                description: 'Avoid increasing retry frequency while the broker is still unhealthy.',
              },
              {
                title: 'Drain gradually',
                description: 'Watch outbox age, lag, downstream saturation, retries, and duplicates.',
              },
            ],
          },
        ],
      },
      {
        id: 'lag',
        title: 'Consumer lag is rising',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Compare ingress rate, completion rate, and handler duration.',
              'Identify the specific route, durable consumer, queue, or partition.',
              'Check downstream database and API latency before adding replicas.',
              'Verify broker throttling, disk pressure, partition distribution, and prefetch.',
              'Extend retention or reduce producers before the backlog reaches the retention boundary.',
            ],
          },
        ],
      },
      {
        id: 'dead-letter-growth',
        title: 'Dead-letter volume is rising',
        blocks: [
          {
            type: 'steps',
            items: [
              {
                title: 'Sample safely',
                description: 'Inspect redacted metadata and authorized payload samples only.',
              },
              {
                title: 'Classify',
                description: 'Separate malformed data, incompatible schema, business rejection, and infrastructure failure.',
              },
              {
                title: 'Stop replay',
                description: 'Disable automated replay if the same failure repeats.',
              },
              {
                title: 'Fix and canary',
                description: 'Deploy the correction and replay a small controlled set.',
              },
              {
                title: 'Scale replay carefully',
                description: 'Rate-limit recovery and watch duplicate suppression and downstream capacity.',
              },
            ],
          },
        ],
      },
      {
        id: 'duplicate-storm',
        title: 'Duplicate-delivery storm',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Verify acknowledgement or commit calls are succeeding.',
              'Check handler duration against visibility or acknowledgement timeouts.',
              'Inspect inbox leases and idempotency expiration.',
              'Reduce concurrency if side effects are timing out under load.',
              'Confirm reconnect or rebalance loops are not abandoning in-flight work.',
              'Never disable duplicate protection to make the queue appear to drain.',
            ],
          },
        ],
      },
    ],
  },
];
