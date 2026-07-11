import type { Metadata } from 'next';
import Link from 'next/link';

import {
  ActivityIcon,
  BookIcon,
  ChevronRightIcon,
  DatabaseIcon,
  PackageIcon,
} from '@/components/icons';
import { docCategories } from '@/lib/docs';

export const metadata: Metadata = {
  title: 'Documentation',
  description:
    'Guides and reference for PodBus messaging, durable jobs, reliability, transports, integrations, and production operations.',
};

const paths = [
  {
    title: 'Build your first service',
    text: 'Install PodBus, connect NATS, publish an event, and register a durable worker.',
    href: '/docs/quick-start',
    icon: BookIcon,
  },
  {
    title: 'Design for failure',
    text: 'Understand delivery semantics, duplicates, outbox, inbox, and idempotency.',
    href: '/docs/delivery-semantics',
    icon: DatabaseIcon,
  },
  {
    title: 'Choose a transport',
    text: 'Compare NATS Core, JetStream, RabbitMQ, and the experimental Kafka adapter.',
    href: '/docs/capability-matrix',
    icon: PackageIcon,
  },
  {
    title: 'Operate in production',
    text: 'Configure health, graceful shutdown, testing, deployment, and incident response.',
    href: '/docs/production-deployment',
    icon: ActivityIcon,
  },
];

export default function DocsOverviewPage() {
  return (
    <div className="docs-overview">
      <header className="docs-overview-hero">
        <span>PodBus documentation</span>
        <h1>Build message-driven Dart services with explicit guarantees.</h1>
        <p>
          Start with the application API, then follow the delivery path through
          broker behavior, database consistency, observability, deployment, and
          recovery.
        </p>
      </header>

      <section className="docs-path-grid" aria-label="Documentation paths">
        {paths.map((path) => {
          const Icon = path.icon;
          return (
            <Link key={path.title} href={path.href}>
              <div><Icon /></div>
              <h2>{path.title}</h2>
              <p>{path.text}</p>
              <span>Open guide <ChevronRightIcon /></span>
            </Link>
          );
        })}
      </section>

      <section className="docs-index-section">
        <div className="docs-index-heading">
          <span>Complete index</span>
          <h2>Documentation by topic</h2>
        </div>

        <div className="docs-index-grid">
          {docCategories.map((category) => (
            <section key={category.title} className="docs-index-category">
              <header>
                <h3>{category.title}</h3>
                <span>{category.pages.length} guides</span>
              </header>
              <nav aria-label={category.title}>
                {category.pages.map((page) => (
                  <Link key={page.slug} href={`/docs/${page.slug}`}>
                    <span>
                      <strong>{page.title}</strong>
                      <small>{page.description}</small>
                    </span>
                    <ChevronRightIcon />
                  </Link>
                ))}
              </nav>
            </section>
          ))}
        </div>
      </section>
    </div>
  );
}
