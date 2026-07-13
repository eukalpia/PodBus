import type { DocPage } from '@/lib/docs-types';

export const qualificationDocs: DocPage[] = [
  {
    slug: 'beta-qualification',
    title: 'Beta qualification',
    description:
      'The exact gates, workloads, guarantees, and limits behind the PodBus beta candidate.',
    category: 'Operations',
    order: 4,
    sections: [
      {
        id: 'status',
        title: 'What beta candidate means',
        blocks: [
          {
            type: 'paragraph',
            text: 'PodBus has completed the listed static, integration, compatibility, deployment, transport, fault, and soak gates on one pinned revision. This is evidence for the tested code paths and environment, not a universal production-readiness certificate.',
          },
          {
            type: 'note',
            tone: 'warning',
            title: 'Application qualification is still required',
            text: 'Run capacity, security, retention, recovery, and downstream-failure tests against the brokers, databases, payloads, and side effects used by the real application.',
          },
        ],
      },
      {
        id: 'workloads',
        title: 'Authoritative transport workloads',
        blocks: [
          {
            type: 'table',
            headers: ['Transport and mode', 'Messages', 'Result', 'Throughput'],
            rows: [
              [
                'NATS Core queue group',
                '1,000,000',
                '1,000,000 unique; 0 duplicates',
                '42,501.6 msg/s',
              ],
              [
                'JetStream durable, memory storage',
                '250,000',
                '250,000 unique; 0 duplicates',
                '2,928.9 msg/s',
              ],
              [
                'JetStream worker, file storage',
                '250,000',
                '250,000 unique; 0 duplicates',
                '2,371.2 msg/s',
              ],
              [
                'RabbitMQ non-persistent, confirmed',
                '1,000,000',
                '1,000,000 received',
                '5,040.8 msg/s',
              ],
              [
                'RabbitMQ persistent, confirmed',
                '500,000',
                '500,000 received',
                '1,705.5 msg/s',
              ],
              [
                'RabbitMQ durable workers',
                '250,000',
                '250,000 received',
                '1,404.6 msg/s',
              ],
            ],
          },
          {
            type: 'paragraph',
            text: 'All rows used 256-byte payloads on GitHub-hosted Ubuntu runners with four logical CPUs and Dart 3.12.0. The values are regression signals, not promises for arbitrary broker configurations. NATS Core, persistent RabbitMQ, and JetStream expose different durability and acknowledgement contracts and should not be ranked as one synthetic race.',
          },
        ],
      },
      {
        id: 'runtime-fixes',
        title: 'Concurrency fixes proven by the matrix',
        blocks: [
          {
            type: 'table',
            headers: ['Path', 'Failure discovered', 'PodBus design'],
            rows: [
              [
                'RabbitMQ publisher confirms',
                'The dart_amqp 0.3.1 multi-ack path mutates confirmation state during iteration under concurrent publishing.',
                'A configurable publisher-lane pool keeps one outstanding confirm per AMQP channel and distributes work across channels.',
              ],
              [
                'JetStream PubAck',
                'dart_nats 1.1.1 serializes request/reply through one global mutex, collapsing concurrent publication into one queue.',
                'A wildcard reply inbox routes confirmations by unique reply subject and permits out-of-order completion.',
              ],
              [
                'NATS and JetStream stress clients',
                'Publisher and consumer sockets sharing one isolate could starve socket reads and misrepresent broker capacity.',
                'Publisher and consumers run in independent isolates with readiness barriers and exact bitmap verification.',
              ],
            ],
          },
        ],
      },
      {
        id: 'faults',
        title: 'Failure qualification',
        blocks: [
          {
            type: 'bullets',
            items: [
              'NATS and RabbitMQ TCP partitions and recovery.',
              'RabbitMQ publisher and consumer channel failures.',
              'Process crashes before NATS or RabbitMQ acknowledgement.',
              'Multiple durable-worker replicas.',
              'Broker stop before NATS PubAck or RabbitMQ publisher confirmation.',
              'Shutdown during dead-letter or retry confirmation.',
              'Slow consumers and bounded concurrency.',
            ],
          },
          {
            type: 'paragraph',
            text: 'Each scenario starts clean brokers and Toxiproxy, records structured evidence, captures broker logs, and verifies the expected delivery, failure, redelivery, reconnection, or confirmation behavior.',
          },
        ],
      },
      {
        id: 'soak',
        title: 'One-hour resilience soak',
        blocks: [
          {
            type: 'paragraph',
            text: 'The soak runs NATS and RabbitMQ continuously with disruption and recovery. The gate requires no missing acknowledged messages, no unhandled asynchronous errors, bounded recovery, successful shutdown, and retained broker evidence.',
          },
          {
            type: 'note',
            tone: 'info',
            title: 'Detector jobs are not evidence',
            text: 'Only the workload job that actually runs for the configured duration counts as soak qualification.',
          },
        ],
      },
      {
        id: 'limits',
        title: 'Known limits',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Package versions remain pre-1.0 and APIs may still change.',
              'Packages are not yet published to pub.dev.',
              'Kafka integration is covered, but Kafka large-stress and rebalance behavior remain experimental.',
              'Cluster, multi-region, and application-side-effect failures require deployment-specific testing.',
              'Exactly-once external side effects are not claimed; use idempotency and transactional boundaries.',
            ],
          },
        ],
      },
    ],
  },
];
