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

const workerCode = `final jobs = NatsJetStreamJobQueue(
  config: NatsMessagingConfig(
    servers: [Uri.parse('nats://localhost:4222')],
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
    text: 'Tracing, bounded metrics, redacted logs, health probes, graceful drain, and failure-oriented tests.',
  },
  {
    icon: BookIcon,
    title: 'Documented guarantees',
    text: 'At-least-once delivery, retry boundaries, dead-letter behavior, and schema evolution are explicit.',
  },
];

const transports = [
  {
    name: 'NATS Core',
    status: 'Beta',
    slug: 'nats-core',
    text: 'Low-latency events, queue groups, and request/reply where broker persistence is not required.',
  },
  {
    name: 'JetStream',
    status: 'Beta',
    slug: 'jetstream',
    text: 'Durable workers with retained messages, PubAck, acknowledgement, redelivery, and consumer state.',
  },
  {
    name: 'RabbitMQ',
    status: 'Beta',
    slug: 'rabbitmq',
    text: 'Topic routing, publisher-confirm lanes, bounded consumers, TTL/DLX retries, and dead letters.',
  },
  {
    name: 'Kafka',
    status: 'Experimental',
    slug: 'kafka',
    text: 'Append-only event streams and consumer groups through PodBus-owned librdkafka bindings.',
  },
];

const qualificationProofs = [
  {
    title: '3.25 million',
    text: 'Mandatory NATS Core, JetStream, and RabbitMQ messages completed on the qualified revision.',
  },
  {
    title: '12 fault paths',
    text: 'Partitions, crashes, channel failures, confirmation loss, replicas, and slow consumers.',
  },
  {
    title: '61m 41.859s',
    text: 'One continuous disruption soak with 28 injected faults and zero missing acknowledged messages.',
  },
  {
    title: '2 Dart tracks',
    text: 'Static analysis and unit tests run on Dart 3.12.0 and the current stable SDK.',
  },
];

const benchmarkResults = [
  {
    transport: 'NATS Core',
    mode: 'Queue group · isolated publisher and consumers',
    messages: '1,000,000 unique · 0 duplicates',
    throughput: '42,501.6 msg/s',
    elapsed: '23.528 s',
  },
  {
    transport: 'JetStream',
    mode: 'Memory · PubAck + manual ack',
    messages: '250,000 unique · 0 duplicates',
    throughput: '2,928.9 msg/s',
    elapsed: '85.356 s',
  },
  {
    transport: 'JetStream',
    mode: 'File worker · PubAck + manual ack',
    messages: '250,000 unique · 0 duplicates',
    throughput: '2,371.2 msg/s',
    elapsed: '105.432 s',
  },
  {
    transport: 'RabbitMQ',
    mode: 'Non-persistent · publisher confirms',
    messages: '1,000,000 received',
    throughput: '5,040.8 msg/s',
    elapsed: '198.379 s',
  },
  {
    transport: 'RabbitMQ',
    mode: 'Persistent queue/messages · confirms',
    messages: '500,000 received',
    throughput: '1,705.5 msg/s',
    elapsed: '293.165 s',
  },
  {
    transport: 'RabbitMQ',
    mode: 'Durable workers · confirms + manual ack',
    messages: '250,000 received',
    throughput: '1,404.6 msg/s',
    elapsed: '177.982 s',
  },
];

const soakResults = [
  {
    title: '0 missing',
    text: '25,004 acknowledged enqueues and 25,004 unique deliveries on both JetStream and RabbitMQ.',
  },
  {
    title: '28 faults',
    text: 'Alternating NATS partitions, RabbitMQ partitions, and RabbitMQ broker restarts.',
  },
  {
    title: '13.401 s p95',
    text: 'Recovery latency p50 was 967 ms; the maximum observed recovery was 13.704 seconds.',
  },
  {
    title: '+52.2 MiB RSS',
    text: 'Memory growth stayed far below the configured 512 MiB qualification threshold.',
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
              <span>Delivery guarantees, made explicit.</span>
            </h1>
            <p>
              Events, request/reply, durable workers, retries, dead letters, and
              database-backed delivery patterns for Dart and Serverpod—without hiding
              the semantics of the broker underneath.
            </p>
            <div className="hero-actions">
              <Link className="primary-button" href="/docs/quick-start">
                Get started <ChevronRightIcon />
              </Link>
              <Link className="secondary-button" href="/docs/beta-qualification">
                View qualification <ChevronRightIcon />
              </Link>
              <a className="secondary-button" href={siteConfig.repository}>
                View source <ArrowUpRightIcon />
              </a>
            </div>
            <div className="hero-proof" aria-label="Supported infrastructure">
              <span>NATS</span><i />
              <span>RabbitMQ</span><i />
              <span>Kafka</span><i />
              <span>PostgreSQL</span>
            </div>
          </div>

          <div className="hero-code">
            <div className="hero-code-logo">
              <BrandLogo className="hero-brand-logo" />
              <span>JetStream · durable worker</span>
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
          <div><strong>{documentationCount}</strong><span>documentation guides</span></div>
          <div><strong>0</strong><span>exactly-once claims</span></div>
        </div>
      </section>

      <section className="home-section home-surface-section">
        <div className="page-shell">
          <header className="home-heading docs-home-heading">
            <div>
              <span>0.1.0-beta.1</span>
              <h2>Evidence before adjectives.</h2>
            </div>
            <p>
              Every beta claim is tied to an explicit workload, acknowledgement
              contract, clean broker environment, and retained CI evidence.
            </p>
          </header>
          <div className="principle-grid">
            {qualificationProofs.map((proof) => (
              <article key={proof.title} className="principle-card">
                <h3>{proof.title}</h3>
                <p>{proof.text}</p>
              </article>
            ))}
          </div>
          <Link className="text-link" href="/docs/beta-qualification">
            Inspect the complete qualification <ChevronRightIcon />
          </Link>
        </div>
      </section>

      <section className="home-section">
        <div className="page-shell">
          <header className="home-heading docs-home-heading">
            <div>
              <span>Measured baseline</span>
              <h2>Six completed transport profiles.</h2>
            </div>
            <p>
              GitHub-hosted Ubuntu, four logical CPUs, Dart 3.12.0, and 256-byte
              payloads. Results are regression evidence—not cross-broker promises.
            </p>
          </header>
          <div className="principle-grid">
            {benchmarkResults.map((result) => (
              <article
                key={`${result.transport}-${result.mode}`}
                className="principle-card"
              >
                <span className="section-label">{result.transport}</span>
                <h3>{result.throughput}</h3>
                <p>{result.mode}</p>
                <p><strong>{result.messages}</strong> · {result.elapsed}</p>
              </article>
            ))}
          </div>
          <Link className="text-link" href="/docs/beta-qualification">
            Read the benchmark methodology <ChevronRightIcon />
          </Link>
        </div>
      </section>

      <section className="home-section home-surface-section">
        <div className="page-shell">
          <header className="home-heading docs-home-heading">
            <div>
              <span>One-hour resilience soak</span>
              <h2>Disruption without silent loss.</h2>
            </div>
            <p>
              61 minutes 41.859 seconds of continuous JetStream and RabbitMQ traffic,
              with partitions and broker restarts injected throughout the run.
            </p>
          </header>
          <div className="principle-grid">
            {soakResults.map((result) => (
              <article key={result.title} className="principle-card">
                <h3>{result.title}</h3>
                <p>{result.text}</p>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section className="home-section">
        <div className="page-shell">
          <header className="home-heading">
            <span>Design</span>
            <h2>One API. Explicit behavior.</h2>
            <p>
              PodBus handles the common mechanics of message-driven services without
              turning distinct broker models into one vague promise.
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
        <div className="page-shell split-section">
          <div>
            <span className="section-label">Capability model</span>
            <h2>The abstraction has a boundary.</h2>
            <p>
              A library can normalize method names. It cannot make NATS Core
              persistent, turn RabbitMQ into an append-only log, or give Kafka
              request/reply semantics. Applications can require capabilities during
              startup and fail before serving traffic.
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

      <section className="home-section">
        <div className="page-shell">
          <header className="home-heading">
            <span>Transports</span>
            <h2>Choose the broker for the workload.</h2>
            <p>
              Start from delivery, replay, routing, and failure requirements. Treat
              throughput as one constraint, not the architecture.
            </p>
          </header>

          <div className="transport-grid">
            {transports.map((transport) => (
              <Link key={transport.name} href={`/docs/${transport.slug}`} className="transport-card">
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
              Durable processing is at-least-once. PodBus documents the crash window
              and provides outbox, inbox, idempotency, retry, and dead-letter tools to
              handle it deliberately.
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
              The documentation covers implementation, production deployment,
              incident response, recovery, and compatibility—not only the first
              successful publish.
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
              Start with a qualified adapter, require the capabilities your service
              depends on, and keep business side effects idempotent.
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
