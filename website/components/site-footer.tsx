import Link from 'next/link';

import { GithubIcon } from '@/components/icons';
import { siteConfig, withBasePath } from '@/lib/site';

export function SiteFooter() {
  return (
    <footer className="site-footer">
      <div className="footer-shell">
        <div className="footer-brand">
          <img src={withBasePath('/podbus-wordmark.svg')} alt="PodBus" />
          <p>Transport-aware messaging and durable jobs for Dart and Serverpod.</p>
          <span>Apache License 2.0</span>
        </div>

        <div className="footer-links">
          <section>
            <h2>Documentation</h2>
            <Link href="/docs/introduction">Introduction</Link>
            <Link href="/docs/quick-start">Quick start</Link>
            <Link href="/docs/delivery-semantics">Reliability</Link>
            <Link href="/docs/api-reference">API reference</Link>
          </section>
          <section>
            <h2>Transports</h2>
            <Link href="/docs/nats-core">NATS Core</Link>
            <Link href="/docs/jetstream">JetStream</Link>
            <Link href="/docs/rabbitmq">RabbitMQ</Link>
            <Link href="/docs/kafka">Kafka</Link>
          </section>
          <section>
            <h2>Project</h2>
            <a href={siteConfig.repository}>Repository</a>
            <a href={`${siteConfig.repository}/releases`}>Releases</a>
            <a href={siteConfig.issues}>Issues</a>
            <a href={`${siteConfig.repository}/blob/main/SECURITY.md`}>Security</a>
          </section>
        </div>
      </div>

      <div className="footer-bottom">
        <span>PodBus {siteConfig.version}</span>
        <a href={siteConfig.repository} aria-label="PodBus on GitHub">
          <GithubIcon />
          GitHub
        </a>
      </div>
    </footer>
  );
}
