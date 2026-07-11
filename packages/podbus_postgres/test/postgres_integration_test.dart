import 'dart:async';
import 'dart:io';

import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_postgres/podbus_postgres.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

void main() {
  group(
    'PostgreSQL reliability integration',
    () {
      late Pool<void> pool;
      late PostgresMessagingSchema schema;

      setUp(() async {
        pool = Pool.withUrl(
          Platform.environment['PODBUS_POSTGRES_URL'] ??
              'postgresql://podbus:podbus@localhost:5432/podbus'
                  '?sslmode=disable&max_connection_count=4',
        );
        final suffix = DateTime.now().microsecondsSinceEpoch;
        schema = PostgresMessagingSchema(
          outboxTable: 'podbus_outbox_$suffix',
          inboxTable: 'podbus_inbox_$suffix',
          idempotencyTable: 'podbus_idempotency_$suffix',
        );
        await pool.run(schema.install);
      });

      tearDown(() async {
        await pool.close(force: true);
      });

      test('persists idempotency claims across store instances', () async {
        final first = PostgresIdempotencyStore(pool, schema: schema);
        final second = PostgresIdempotencyStore(pool, schema: schema);

        expect(
          await first.claim('lead:42', ttl: const Duration(minutes: 1)),
          isTrue,
        );
        expect(
          await second.claim('lead:42', ttl: const Duration(minutes: 1)),
          isFalse,
        );
        await first.release('lead:42');
        expect(
          await second.claim('lead:42', ttl: const Duration(minutes: 1)),
          isTrue,
        );
      });

      test('publishes a transactionally inserted outbox message', () async {
        final bus = InMemoryMessageBus();
        await bus.connect();
        addTearDown(bus.close);

        final received = Completer<Map<String, Object?>>();
        await bus.subscribe<Map<String, Object?>>(
          'lead.created',
          handler: (_, payload) async => received.complete(payload),
        );

        final outbox = PostgresOutbox(pool, schema: schema);
        await pool.runTx((transaction) async {
          await transaction.execute('''
CREATE TABLE IF NOT EXISTS podbus_test_leads (
  id integer PRIMARY KEY
)
''');
          await transaction.execute(
            Sql.named('''
INSERT INTO podbus_test_leads (id)
VALUES (@id:int4)
ON CONFLICT (id) DO NOTHING
'''),
            parameters: {'id': 42},
          );
          await outbox.enqueue(
            transaction,
            'lead.created',
            {'leadId': 42},
            headers: MessageHeaders(correlationId: 'corr-postgres'),
          );
        });

        final relay = PostgresOutboxRelay(
          outbox: outbox,
          bus: bus,
          workerId: 'integration-relay',
        );
        expect(await relay.runOnce(), 1);
        expect(await received.future.timeout(const Duration(seconds: 3)), {
          'leadId': 42,
        });
      });

      test(
        'prevents a completed inbox message from being reacquired',
        () async {
          final inbox = PostgresInbox(pool, schema: schema);
          final lease = await inbox.acquire('message-42', workerId: 'worker-a');
          expect(lease, isNotNull);
          await inbox.complete(lease!);
          expect(
            await inbox.acquire('message-42', workerId: 'worker-b'),
            isNull,
          );
        },
      );
    },
    tags: 'integration',
    timeout: Timeout(const Duration(minutes: 1)),
    skip: _integrationSkip,
  );
}

Object? get _integrationSkip {
  if (Platform.environment['PODBUS_RUN_INTEGRATION_TESTS'] == 'true') {
    return false;
  }
  return 'Set PODBUS_RUN_INTEGRATION_TESTS=true to run Docker-backed tests.';
}
