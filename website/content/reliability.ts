import type { DocPage } from '@/lib/docs-types';

export const reliabilityDocs: DocPage[] = [
  {
    slug: 'delivery-semantics',
    title: 'Delivery semantics',
    description:
      'Understand acknowledgement, redelivery, duplicates, ordering, and the exact guarantees PodBus does not claim.',
    category: 'Reliability',
    order: 1,
    sections: [
      {
        id: 'at-least-once',
        title: 'At-least-once delivery',
        blocks: [
          {
            type: 'paragraph',
            text: 'Broker-backed PodBus workers use at-least-once delivery. A message may be delivered more than once when a process crashes after performing a side effect but before acknowledgement, when an acknowledgement is lost, when a lease expires, or when a consumer is rebalanced.',
          },
          {
            type: 'note',
            tone: 'info',
            title: 'The duplicate is part of the contract',
            text: 'Design handlers so that processing the same message again is safe. Do not treat duplicates as a rare broker defect.',
          },
        ],
      },
      {
        id: 'critical-window',
        title: 'The critical failure window',
        blocks: [
          {
            type: 'code',
            language: 'text',
            code: `receive message
      │
      ▼
perform business side effect ─── process crashes here
      │
      ▼
acknowledge or commit message`,
          },
          {
            type: 'paragraph',
            text: 'If the side effect commits and the process dies before the broker records the acknowledgement, the broker redelivers the message. No generic messaging abstraction can make an unrelated database, payment provider, email server, and broker participate in one atomic transaction.',
          },
        ],
      },
      {
        id: 'ordering',
        title: 'Ordering',
        blocks: [
          {
            type: 'paragraph',
            text: 'Ordering is determined by the broker, route, partitioning strategy, consumer concurrency, retries, and failure handling. PodBus does not promise global ordering.',
          },
          {
            type: 'table',
            headers: ['Situation', 'Effect on order'],
            rows: [
              ['Concurrency greater than one', 'Handlers may complete in a different order than messages arrived.'],
              ['Retry with delay', 'A later message may complete before an earlier failed message.'],
              ['Kafka partitions', 'Order is normally scoped to one partition.'],
              ['NATS queue groups', 'Messages are distributed among subscribers; completion order is not global.'],
              ['RabbitMQ multiple consumers', 'Delivery order and completion order may diverge.'],
            ],
          },
        ],
      },
      {
        id: 'exactly-once',
        title: 'Why PodBus does not claim exactly-once',
        blocks: [
          {
            type: 'paragraph',
            text: 'Exactly-once can describe a narrow system boundary, such as one Kafka transaction or one database transaction. It does not automatically extend to arbitrary external side effects. PodBus therefore documents acknowledgement and idempotency behavior instead of using exactly-once as a blanket claim.',
          },
          {
            type: 'bullets',
            items: [
              'Use a transactional outbox for database state plus outgoing messages.',
              'Use an inbox or persistent idempotency store around incoming side effects.',
              'Use provider-level idempotency keys for payments and other remote APIs.',
              'Keep handler outputs deterministic where practical.',
              'Record message IDs with externally visible operations.',
            ],
          },
        ],
      },
      {
        id: 'failure-matrix',
        title: 'Failure matrix',
        blocks: [
          {
            type: 'table',
            headers: ['Failure point', 'Expected result'],
            rows: [
              ['Before handler starts', 'Message is available for another delivery.'],
              ['During handler before side effect commits', 'Handler fails; retry or dead-letter policy applies.'],
              ['After side effect, before acknowledgement', 'Message may be redelivered; idempotency must suppress duplication.'],
              ['During retry publication', 'Source message must remain unacknowledged until retry publication is confirmed.'],
              ['During dead-letter publication', 'Source message must not be finalized until the dead-letter write is confirmed.'],
              ['During graceful shutdown', 'Active handlers drain until the configured deadline.'],
            ],
          },
        ],
      },
    ],
  },
  {
    slug: 'idempotency',
    title: 'Idempotency',
    description:
      'Protect business side effects from duplicate delivery across processes and restarts.',
    category: 'Reliability',
    order: 2,
    sections: [
      {
        id: 'definition',
        title: 'What idempotency means',
        blocks: [
          {
            type: 'paragraph',
            text: 'An idempotent operation can be attempted multiple times without producing additional externally visible effects after the first successful execution. The implementation usually associates one stable key with one business operation.',
          },
          {
            type: 'code',
            language: 'text',
            code: `message id:      01JABC...
business key:    invoice:inv-42:issue
provider key:    payment:order-42:capture`,
          },
        ],
      },
      {
        id: 'key-selection',
        title: 'Choose the right key',
        blocks: [
          {
            type: 'table',
            headers: ['Key type', 'Use when', 'Caution'],
            rows: [
              ['Message ID', 'Every physical message should be processed once.', 'A replay with a new ID may bypass it.'],
              ['Business operation key', 'The same logical action may arrive in different messages.', 'Define the operation boundary precisely.'],
              ['Aggregate version', 'Applying state transitions to one entity.', 'Requires monotonic version rules.'],
              ['Provider idempotency key', 'Calling a remote API that supports deduplication.', 'Retention and semantics belong to the provider.'],
            ],
          },
        ],
      },
      {
        id: 'persistent-store',
        title: 'Persistent idempotency store',
        blocks: [
          {
            type: 'paragraph',
            text: 'An in-memory set protects one process only. Production replicas need a shared store such as PostgreSQL. The claim must survive restarts, support expiration, and recover from a worker that dies while holding a claim.',
          },
          {
            type: 'code',
            language: 'dart',
            code: `final store = PostgresIdempotencyStore(pool);

final claimed = await store.claim(
  'invoice:inv-42:issue',
  ttl: const Duration(days: 7),
);

if (!claimed) {
  return; // already completed or currently owned
}

try {
  await issueInvoice('inv-42');
} catch (_) {
  await store.release('invoice:inv-42:issue');
  rethrow;
}`,
          },
        ],
      },
      {
        id: 'state-machine',
        title: 'Recommended state machine',
        blocks: [
          {
            type: 'table',
            headers: ['State', 'Meaning'],
            rows: [
              ['pending', 'Known but not currently owned.'],
              ['processing', 'Owned by a worker until a lease deadline.'],
              ['completed', 'The protected operation succeeded.'],
              ['failed', 'The operation reached a terminal failure.'],
              ['expired', 'Retention elapsed and the key may be reclaimed.'],
            ],
          },
          {
            type: 'paragraph',
            text: 'A lease is safer than a permanent processing flag. If a worker dies, another worker can recover the operation after the lease expires.',
          },
        ],
      },
      {
        id: 'side-effect-patterns',
        title: 'Side-effect patterns',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Database insert: use a unique business key and handle conflict as already completed.',
              'Database update: include an expected aggregate version.',
              'Payment capture: pass a provider idempotency key and store the provider result.',
              'Email: store a sent marker keyed by template and recipient business operation.',
              'Webhook: keep an inbox record and return success for known duplicate event IDs.',
              'Object storage: write to a deterministic object key and compare checksum.',
            ],
          },
        ],
      },
    ],
  },
  {
    slug: 'transactional-outbox',
    title: 'Transactional outbox',
    description:
      'Publish database-backed events without a gap between business state and broker delivery.',
    category: 'Reliability',
    order: 3,
    sections: [
      {
        id: 'dual-write',
        title: 'The dual-write problem',
        blocks: [
          {
            type: 'code',
            language: 'dart',
            code: `await database.insert(order); // succeeds
await bus.publish('order.created', order); // process crashes`,
          },
          {
            type: 'paragraph',
            text: 'The database now contains the order, but the event never reached the broker. Reversing the order creates the opposite failure: the event is visible while the database transaction later fails.',
          },
        ],
      },
      {
        id: 'pattern',
        title: 'Outbox pattern',
        blocks: [
          {
            type: 'steps',
            items: [
              {
                title: 'Begin one database transaction',
                description: 'The business record and outbox record share the same commit boundary.',
              },
              {
                title: 'Write business state',
                description: 'Insert or update the aggregate using normal repository code.',
              },
              {
                title: 'Append the outgoing message',
                description: 'Store route, payload, headers, key, and timestamps in the outbox table.',
              },
              {
                title: 'Commit',
                description: 'Either both records become visible or neither does.',
              },
              {
                title: 'Relay asynchronously',
                description: 'A separate worker leases pending rows, publishes, waits for confirmation, and marks them published.',
              },
            ],
          },
        ],
      },
      {
        id: 'example',
        title: 'Write business state and event together',
        blocks: [
          {
            type: 'code',
            language: 'dart',
            code: `final outbox = PostgresOutbox(pool);

await pool.runTx((transaction) async {
  await OrderRepository.insert(transaction, order);

  await outbox.enqueue(
    transaction,
    'order.created',
    order.toJson(),
    key: order.id,
    headers: MessageHeaders(
      correlationId: requestId,
      causationId: commandId,
    ),
  );
});`,
          },
        ],
      },
      {
        id: 'relay',
        title: 'Run the relay',
        blocks: [
          {
            type: 'code',
            language: 'dart',
            code: `final relay = PostgresOutboxRelay(
  outbox: outbox,
  bus: bus,
  workerId: 'orders-api-\${Platform.localHostname}',
);

while (!shuttingDown) {
  final published = await relay.runOnce();
  if (published == 0) {
    await Future<void>.delayed(const Duration(milliseconds: 250));
  }
}`,
          },
          {
            type: 'paragraph',
            text: 'Relay replicas use leases and row locking so multiple instances can share the same table. Publication remains at-least-once: a crash after broker confirmation but before the database update can publish the same outbox record again.',
          },
        ],
      },
      {
        id: 'operations',
        title: 'Operate the outbox',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Alert on the age of the oldest pending record, not only row count.',
              'Bound attempts and preserve the last redacted error.',
              'Recover expired leases automatically.',
              'Archive or delete published rows according to an explicit retention policy.',
              'Keep the outbox in the same database transaction as the business state.',
              'Scale relays only after checking broker and database capacity.',
            ],
          },
        ],
      },
    ],
  },
  {
    slug: 'inbox-processing',
    title: 'Inbox processing',
    description:
      'Deduplicate incoming messages and recover work abandoned by failed consumers.',
    category: 'Reliability',
    order: 4,
    sections: [
      {
        id: 'purpose',
        title: 'Why an inbox exists',
        blocks: [
          {
            type: 'paragraph',
            text: 'An inbox records incoming message processing in a shared database. It lets replicas coordinate duplicate suppression and lets a new worker recover a message when the previous worker died after acquiring it.',
          },
        ],
      },
      {
        id: 'lease-flow',
        title: 'Lease flow',
        blocks: [
          {
            type: 'steps',
            items: [
              {
                title: 'Acquire',
                description: 'Insert or claim the message ID with a worker ID and lease deadline.',
              },
              {
                title: 'Process',
                description: 'Perform the side effect while the lease is valid.',
              },
              {
                title: 'Complete',
                description: 'Mark the inbox record completed in the same transaction as local state where possible.',
              },
              {
                title: 'Recover',
                description: 'A later worker may acquire an expired processing lease.',
              },
            ],
          },
        ],
      },
      {
        id: 'example',
        title: 'Consumer-side deduplication',
        blocks: [
          {
            type: 'code',
            language: 'dart',
            code: `final inbox = PostgresInbox(pool);

handler: (context, event) async {
  final messageId = context.headers.messageId;
  final lease = await inbox.acquire(
    messageId,
    workerId: workerId,
  );

  if (lease == null) {
    return; // completed or currently leased elsewhere
  }

  try {
    await applyProjection(event);
    await inbox.complete(lease);
  } catch (error) {
    await inbox.fail(lease, error: error);
    rethrow;
  }
}`, 
          },
        ],
      },
      {
        id: 'transaction-boundary',
        title: 'Choose the transaction boundary',
        blocks: [
          {
            type: 'paragraph',
            text: 'When the side effect is in the same PostgreSQL database, update business state and complete the inbox in one transaction. When the side effect is remote, combine the inbox with a provider idempotency key and store the provider result.',
          },
          {
            type: 'note',
            tone: 'warning',
            title: 'Completed too early is data loss',
            text: 'Do not mark an inbox record completed before the protected side effect commits. A crash in that gap suppresses future delivery while the work never happened.',
          },
        ],
      },
      {
        id: 'retention',
        title: 'Retention',
        blocks: [
          {
            type: 'paragraph',
            text: 'Keep completed inbox records for at least the maximum period in which the same message may be redelivered or replayed. Broker retention, backup restoration, manual replay, and provider retry windows all affect this value.',
          },
        ],
      },
    ],
  },
  {
    slug: 'schema-evolution',
    title: 'Schema evolution',
    description:
      'Change message contracts without breaking retained events or rolling deployments.',
    category: 'Reliability',
    order: 5,
    sections: [
      {
        id: 'compatibility-window',
        title: 'Compatibility window',
        blocks: [
          {
            type: 'paragraph',
            text: 'A consumer may receive messages produced by old deployments, restored backups, manual replays, or delayed jobs. Keep decoders for every schema version that can still appear within the operational replay window.',
          },
        ],
      },
      {
        id: 'safe-changes',
        title: 'Usually safe changes',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Add an optional field with a documented default.',
              'Add a new enum value only when old consumers handle unknown values safely.',
              'Relax validation while preserving meaning.',
              'Add metadata that old consumers ignore.',
              'Create a new consumer that reads the existing contract.',
            ],
          },
        ],
      },
      {
        id: 'breaking-changes',
        title: 'Breaking changes',
        blocks: [
          {
            type: 'bullets',
            items: [
              'Removing or renaming a required field.',
              'Changing units, currency, timezone, or identifier semantics.',
              'Changing one field from scalar to collection.',
              'Reusing a message type for a different business event.',
              'Changing ordering or uniqueness assumptions without a new contract.',
            ],
          },
        ],
      },
      {
        id: 'upcasting',
        title: 'Upcasting old messages',
        blocks: [
          {
            type: 'code',
            language: 'dart',
            code: `LeadCreated decodeLeadCreated(
  Map<String, Object?> json,
  int version,
) {
  return switch (version) {
    1 => LeadCreated(
        leadId: json['id']! as int,
        source: 'unknown',
      ),
    2 => LeadCreated.fromJson(json),
    _ => throw MessageCodecException(
        'Unsupported crm.lead-created schema version: $version',
      ),
  };
}`,
          },
          {
            type: 'paragraph',
            text: 'Upcasters should be deterministic and side-effect free. They convert old wire data into the current in-memory model before business logic runs.',
          },
        ],
      },
      {
        id: 'rollout',
        title: 'Safe rollout order',
        blocks: [
          {
            type: 'steps',
            items: [
              {
                title: 'Deploy tolerant consumers',
                description: 'Consumers understand both the current and next schema.',
              },
              {
                title: 'Deploy new producers',
                description: 'Producers begin writing the new version only after consumers are ready.',
              },
              {
                title: 'Wait through retention',
                description: 'Keep old decoders while old messages may still be replayed.',
              },
              {
                title: 'Remove compatibility code deliberately',
                description: 'Record the removed version and minimum supported deployment.',
              },
            ],
          },
        ],
      },
      {
        id: 'fixtures',
        title: 'Compatibility fixtures',
        blocks: [
          {
            type: 'paragraph',
            text: 'Store representative serialized messages from every supported version in source control. Decode them in tests and verify business meaning, not only successful JSON parsing.',
          },
        ],
      },
    ],
  },
];
