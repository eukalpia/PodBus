import Link from 'next/link';

import { BrandLogo } from '@/components/brand-logo';
import { CodeBlock } from '@/components/code-block';
import {
  ActivityIcon,
  ArrowUpRightIcon,
  BookIcon,
  ChevronRightIcon,
  DatabaseIcon,
  PackageIcon,
} from '@/components/icons';
import { docCategories } from '@/lib/docs';
import { siteConfig } from '@/lib/site';

const workerCode = `final jobs = ResilientDurableJobQueue(
  factory: () => NatsJetStreamJobQueue(
    config: natsConfig,
    fetchBatchSize: 64,
  ),
  policy: const ReconnectPolicy(
    maxAttempts: 12,
    recoveryTimeout: Duration(seconds: 30),
    disposeTimeout: Duration(seconds: 2),
  ),
);

await jobs.connect();
await jobs.worker<Map<String, Object?>>(
  'jobs.email.welcome',
  durableName: 'welcome-email-v1',
  concurrency: 8,
  handler: (context, job) async {
    await sendWelcomeEmail(job['email']! as String);
  },
);`;

const principles = [
  {
    icon: PackageIcon,
    title: 'Transport-aware API',
    text: 'Common contracts for application code, with capability checks where broker behavior diverges.',
  },
  {
    icon: DatabaseIcon,
    title: 'Database reliability',
    text: 'Transactional outbox, inbox leases, and persistent idempotency for multi-replica services.',
  },
  {
    icon: ActivityIcon,
    title: 'Operational by design',
    text: 'Reconnect supervision, bounded metrics, redacted logs, health probes, graceful drain, and fault tests.',
  },
  {
    icon: BookIcon,
    title: 'Documented guarantees',
    text: 'At-least-once delivery, retry boundaries, dead-letter behavior, and schema evolution are explicit.',
  },
];

const qualification = [
  {
    icon: ActivityIcon,
    title: '12 isolated fault scenarios',
    text: 'TCP partitions, broker stops, channel failures, crash-before-ack, multi-replica workers, and shutdown races.',
  },
  {
    icon: PackageIcon,
    title: '3.25M-message profile',
    text: 'Mandatory NATS Core, JetStream, and RabbitMQ workloads with exact payload, publisher, and consumer settings.',
  },
  {
    icon: DatabaseIcon,
    title: 'One-hour soak gate',
    text: 'Alternating faults with acknowledged, unique, duplicate, missing-message, recovery-latency, and RSS accounting.',
  },
  {
    icon: BookIcon,
    title: 'Framework-neutral proof',
    text: 'A plain Dart service is started as an independent process, probed through HTTP, and drained cleanly.',
  },
];

const transports = [
  {
    name: 'NATS Core',
    status: 'Reference',
    slug: 'nats-core',
    text: 'Low-latency events, queue groups, and request/reply where broker persistence is not required.',
  },
  {
    name: 'JetStream',
    status: 'Reference',
    slug: 'jetstream',
    text: 'Durable workers with retained messages, acknowledgement windows, heartbeats, redelivery, and consumer state.',
  },
  {
    name: 'RabbitMQ',
    status: 'Beta candidate',
    slug: 'rabbitmq',
    text: 'Topic routing, publisher confirms, bounded consumers, TTL/DLX retries, dead letters, and channel recovery.',
  },
  {
    name: 'Kafka',
    status: 'Experimental',
    slug: 'kafka',
    text: 'Append-only event streams and consumer groups through PodBus-owned librdkafka bindings.',
  },
];

export default function HomePage() {
  const documentationCount = docCategories.reduce(
    (total, category) => total + category.pages.length,
    0,
  );

  return (
    <main id="main-content">
      <section className="home-hero">
        <div className="home-grid" aria-hidden="true" />
        <div className="home-glow home-glow-left" aria-hidden="true" />
        <div className="home-glow home-glow-right" aria-hidden="true" />

        <div className="page-shell hero-layout">
          <div className="hero-copy">
            <h1>
              Messaging for Dart.
              <span>Failure behavior, made explicit.</span>
            </h1>
            <p>
              Events, request/reply, durable workers, reconnect supervision,
              retries, dead letters, and database-backed delivery patterns for
              Dart and Serverpod—without hiding the broker underneath.
            </p>
            <div className="hero-actions">
              <Link className="primary-button" href="/docs/quick-start">
                Get started <ChevronRightIcon />
              </Link>
              <a className="secondary-button" href={siteConfig.repository}>
                View source <ArrowUpRightIcon />
              </a>
            </div>
            <div className="hero-proof" aria-label="Supported infrastructure">
              <span>Beta candidate</span><i />
              <span>NATS</span><i />
              <span>RabbitMQ</span><i />
              <span>PostgreSQL</span>
            </div>
          </div>

          <div className="hero-code">
            <div className="hero-code-logo">
              <BrandLogo className="hero-brand-logo" />
              <span>JetStream · resilient worker</span>
            </div>
            <CodeBlock
              code={workerCode}
              language="dart"
              filename="worker.dart"
            />
            <div className="hero-code-status">
              <span><i /> connected</span>
              <span>at-least-once</span>
            </div>
          </div>
        </div>
      </section>

      <section className="home-stats" aria-label="Project summary">
        <div className="page-shell stats-grid">
          <div><strong>7</strong><span>focused packages</span></div>
          <div><strong>4</strong><span>infrastructure integrations</span></div>
          <div><strong>12</strong><span>isolated fault scenarios</span></div>
          <div><strong>0</strong><span>exactly-once claims</span></div>
        </div>
      </section>

      <section className="home-section">
        <div className="page-shell">
          <header className="home-heading">
            <span>Design</span>
            <h2>One API. Explicit behavior.</h2>
            <p>
              PodBus handles the common mechanics of message-driven services
              without turning distinct broker models into one vague promise.
            </p>
          </header>

          <div className="principle-grid">
            {principles.map((principle) => {
              const Icon = principle.icon;
              return (
                <article key={principle.title} className="principle-card">
                  <div className="principle-icon"><Icon /></div>
                  <h3>{principle.title}</h3>
                  <p>{principle.text}</p>
                </article>
              );
            })}
          </div>
        </div>
      </section>

      <section className="home-section home-surface-section">
        <div className="page-shell">
          <header className="home-heading">
            <span>Qualification</span>
            <h2>Evidence before release labels.</h2>
            <p>
              A green unit suite is the beginning, not the verdict. PodBus keeps
              fault, stress, soak, compatibility, security, and process-level
              evidence separate so a skipped job cannot masquerade as proof.
            </p>
          </header>

          <div className="principle-grid">
            {qualification.map((item) => {
              const Icon = item.icon;
              return (
                <article key={item.title} className="principle-card">
                  <div className="principle-icon"><Icon /></div>
                  <h3>{item.title}</h3>
                  <p>{item.text}</p>
                </article>
              );
            })}
          </div>

          <div className="hero-actions">
            <a
              className="secondary-button"
              href={`${siteConfig.repository}/actions`}
            >
              Inspect workflow evidence <ArrowUpRightIcon />
            </a>
            <a
              className="secondary-button"
              href={`${siteConfig.repository}/blob/main/README.md#reliability-qualification`}
            >
              Read the methodology <ArrowUpRightIcon />
            </a>
          </div>
        </div>
      </section>

      <section className="home-section">
        <div className="page-shell split-section">
          <div>
            <span className="section-label">Capability model</span>
            <h2>The abstraction has a boundary.</h2>
            <p>
              A library can normalize method names. It cannot make NATS Core
              persistent, turn RabbitMQ into an append-only log, or give Kafka
              request/reply semantics. Applications can require capabilities
              during startup and fail before serving traffic.
            </p>
            <Link className="text-link" href="/docs/capabilities">
              Understand capabilities <ChevronRightIcon />
            </Link>
          </div>

          <div className="capability-demo">
            <div className="capability-label">startup.dart</div>
            <pre><code>{`queue.capabilities.requireAll({
  MessagingCapability.durableJobs,
  MessagingCapability.deadLettering,
  MessagingCapability.gracefulShutdown,
});`}</code></pre>
            <div className="capability-result">
              <span /> requirements satisfied before readiness
            </div>
          </div>
        </div>
      </section>

      <section className="home-section home-surface-section">
        <div className="page-shell">
          <header className="home-heading">
            <span>Transports</span>
            <h2>Choose the broker for the workload.</h2>
            <p>
              Start from delivery, replay, routing, and failure requirements.
              Treat throughput as one constraint, not the architecture.
            </p>
          </header>

          <div className="transport-grid">
            {transports.map((transport) => (
              <Link
                key={transport.name}
                href={`/docs/${transport.slug}`}
                className="transport-card"
              >
                <div>
                  <span className={`transport-status ${transport.status.toLowerCase()}`}>
                    {transport.status}
                  </span>
                  <h3>{transport.name}</h3>
                  <p>{transport.text}</p>
                </div>
                <ChevronRightIcon />
              </Link>
            ))}
          </div>
        </div>
      </section>

      <section className="home-section reliability-home">
        <div className="page-shell split-section reverse">
          <div className="reliability-map" aria-label="Reliable processing flow">
            <div><span>01</span><strong>Receive</strong><p>Decode and validate the wire contract.</p></div>
            <div><span>02</span><strong>Protect</strong><p>Acquire inbox or idempotency state.</p></div>
            <div><span>03</span><strong>Process</strong><p>Perform the business side effect.</p></div>
            <div><span>04</span><strong>Finalize</strong><p>Ack, retry, or dead-letter after confirmation.</p></div>
          </div>
          <div>
            <span className="section-label">Reliability</span>
            <h2>No exactly-once theatre.</h2>
            <p>
              Durable processing is at-least-once. PodBus documents the crash
              window and provides outbox, inbox, idempotency, retry, dead-letter,
              and recovery tools to handle it deliberately.
            </p>
            <Link className="text-link" href="/docs/delivery-semantics">
              Read the delivery model <ChevronRightIcon />
            </Link>
          </div>
        </div>
      </section>

      <section className="home-section home-surface-section">
        <div className="page-shell">
          <header className="home-heading docs-home-heading">
            <div>
              <span>Documentation</span>
              <h2>Built for the failure path.</h2>
            </div>
            <p>
              {documentationCount} guides cover implementation, production
              deployment, incident response, recovery, and compatibility—not
              only the first successful publish.
            </p>
          </header>

          <div className="category-grid">
            {docCategories.map((category) => (
              <section key={category.title} className="category-card">
                <div className="category-card-header">
                  <h3>{category.title}</h3>
                  <span>{category.pages.length}</span>
                </div>
                <nav aria-label={category.title}>
                  {category.pages.slice(0, 5).map((page) => (
                    <Link key={page.slug} href={`/docs/${page.slug}`}>
                      {page.title}
                      <ChevronRightIcon />
                    </Link>
                  ))}
                </nav>
                <Link className="category-all" href={`/docs/${category.pages[0].slug}`}>
                  Browse section
                </Link>
              </section>
            ))}
          </div>
        </div>
      </section>

      <section className="home-cta">
        <div className="page-shell">
          <div className="cta-panel">
            <div className="cta-glow" aria-hidden="true" />
            <span>Open source · Apache 2.0</span>
            <h2>Build the message path you can explain during an incident.</h2>
            <p>
              Start from required guarantees, inspect the qualification
              evidence, and keep externally visible side effects idempotent.
            </p>
            <div className="hero-actions">
              <Link className="primary-button" href="/docs/introduction">
                Read documentation <ChevronRightIcon />
              </Link>
              <a className="secondary-button" href={siteConfig.repository}>
                Open GitHub <ArrowUpRightIcon />
              </a>
            </div>
          </div>
        </div>
      </section>
    </main>
  );
}
