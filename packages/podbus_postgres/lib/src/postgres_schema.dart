import 'package:podbus_core/podbus_core.dart';
import 'package:postgres/postgres.dart';

final class PostgresMessagingSchema {
  PostgresMessagingSchema({
    this.outboxTable = 'podbus_outbox',
    this.inboxTable = 'podbus_inbox',
    this.idempotencyTable = 'podbus_idempotency',
  }) {
    _validateIdentifier(outboxTable, 'outboxTable');
    _validateIdentifier(inboxTable, 'inboxTable');
    _validateIdentifier(idempotencyTable, 'idempotencyTable');
  }

  final String outboxTable;
  final String inboxTable;
  final String idempotencyTable;

  Future<void> install(Session session) async {
    await session.execute('''
CREATE TABLE IF NOT EXISTS $outboxTable (
  id text PRIMARY KEY,
  topic text NOT NULL,
  message_key text,
  payload bytea NOT NULL,
  content_type text NOT NULL,
  schema_version integer NOT NULL CHECK (schema_version > 0),
  message_type text,
  headers jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'processing', 'failed', 'published', 'dead')),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  locked_by text,
  locked_until timestamptz,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
)
''');
    await session.execute('''
CREATE INDEX IF NOT EXISTS ${outboxTable}_ready_idx
ON $outboxTable (next_attempt_at, created_at)
WHERE status IN ('pending', 'failed')
''');
    await session.execute('''
CREATE INDEX IF NOT EXISTS ${outboxTable}_lease_idx
ON $outboxTable (locked_until)
WHERE status = 'processing'
''');

    await session.execute('''
CREATE TABLE IF NOT EXISTS $inboxTable (
  message_id text PRIMARY KEY,
  state text NOT NULL
    CHECK (state IN ('processing', 'completed', 'failed')),
  attempts integer NOT NULL DEFAULT 1 CHECK (attempts > 0),
  locked_by text NOT NULL,
  locked_until timestamptz NOT NULL,
  last_error text,
  first_seen_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz
)
''');
    await session.execute('''
CREATE INDEX IF NOT EXISTS ${inboxTable}_lease_idx
ON $inboxTable (locked_until)
WHERE state <> 'completed'
''');

    await session.execute('''
CREATE TABLE IF NOT EXISTS $idempotencyTable (
  idempotency_key text PRIMARY KEY,
  expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
)
''');
    await session.execute('''
CREATE INDEX IF NOT EXISTS ${idempotencyTable}_expiry_idx
ON $idempotencyTable (expires_at)
''');
  }

  static void _validateIdentifier(String value, String field) {
    if (!RegExp(r'^[a-z_][a-z0-9_]*$').hasMatch(value)) {
      throw MessagingConfigurationException(
        '$field must be a lowercase PostgreSQL identifier, got "$value".',
      );
    }
  }
}
