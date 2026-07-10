import 'package:podbus_core/podbus_core.dart';
import 'package:postgres/postgres.dart';

import 'postgres_schema.dart';

enum InboxState { processing, completed, failed }

final class InboxLease {
  const InboxLease({
    required this.messageId,
    required this.workerId,
    required this.attempt,
    required this.lockedUntil,
  });

  final String messageId;
  final String workerId;
  final int attempt;
  final DateTime lockedUntil;
}

final class PostgresInbox {
  PostgresInbox(
    this.executor, {
    PostgresMessagingSchema? schema,
  }) : schema = schema ?? PostgresMessagingSchema();

  final SessionExecutor executor;
  final PostgresMessagingSchema schema;

  Future<void> install() => executor.run(schema.install);

  Future<InboxLease?> acquire(
    String messageId, {
    required String workerId,
    Duration lease = const Duration(minutes: 1),
  }) async {
    if (messageId.trim().isEmpty || workerId.trim().isEmpty) {
      throw const MessagingConfigurationException(
        'Inbox messageId and workerId must not be empty.',
      );
    }
    if (lease <= Duration.zero) {
      throw const MessagingConfigurationException(
        'Inbox lease must be greater than zero.',
      );
    }

    return executor.run((session) async {
      final result = await session.execute(
        Sql.named('''
INSERT INTO ${schema.inboxTable} (
  message_id,
  state,
  attempts,
  locked_by,
  locked_until
)
VALUES (
  @message_id:text,
  'processing',
  1,
  @worker_id:text,
  now() + (@lease_ms:int8 * interval '1 millisecond')
)
ON CONFLICT (message_id) DO UPDATE
SET
  state = 'processing',
  attempts = ${schema.inboxTable}.attempts + 1,
  locked_by = EXCLUDED.locked_by,
  locked_until = EXCLUDED.locked_until,
  last_error = NULL,
  updated_at = now()
WHERE ${schema.inboxTable}.state <> 'completed'
  AND ${schema.inboxTable}.locked_until <= now()
RETURNING message_id, locked_by, attempts, locked_until
'''),
        parameters: {
          'message_id': messageId,
          'worker_id': workerId,
          'lease_ms': lease.inMilliseconds,
        },
      );
      if (result.isEmpty) {
        return null;
      }
      final row = result.first.toColumnMap();
      return InboxLease(
        messageId: row['message_id']! as String,
        workerId: row['locked_by']! as String,
        attempt: row['attempts']! as int,
        lockedUntil: row['locked_until']! as DateTime,
      );
    });
  }

  Future<void> extend(InboxLease lease, Duration duration) async {
    if (duration <= Duration.zero) {
      throw const MessagingConfigurationException(
        'Inbox extension must be greater than zero.',
      );
    }
    final updated = await executor.run((session) async {
      final result = await session.execute(
        Sql.named('''
UPDATE ${schema.inboxTable}
SET
  locked_until = now() + (@lease_ms:int8 * interval '1 millisecond'),
  updated_at = now()
WHERE message_id = @message_id:text
  AND state = 'processing'
  AND locked_by = @worker_id:text
RETURNING message_id
'''),
        parameters: {
          'message_id': lease.messageId,
          'worker_id': lease.workerId,
          'lease_ms': duration.inMilliseconds,
        },
      );
      return result.isNotEmpty;
    });
    if (!updated) {
      throw const MessagingConnectionException(
        'Inbox lease could not be extended because ownership was lost.',
      );
    }
  }

  Future<void> complete(InboxLease lease) async {
    final updated = await _transition(
      lease,
      state: InboxState.completed,
      error: null,
    );
    if (!updated) {
      throw const MessagingConnectionException(
        'Inbox message could not be completed because ownership was lost.',
      );
    }
  }

  Future<void> fail(InboxLease lease, Object error) async {
    final updated = await _transition(
      lease,
      state: InboxState.failed,
      error: error.toString(),
    );
    if (!updated) {
      throw const MessagingConnectionException(
        'Inbox message could not be failed because ownership was lost.',
      );
    }
  }

  Future<bool> _transition(
    InboxLease lease, {
    required InboxState state,
    required String? error,
  }) {
    return executor.run((session) async {
      final result = await session.execute(
        Sql.named('''
UPDATE ${schema.inboxTable}
SET
  state = @state:text,
  locked_until = now(),
  last_error = @last_error:text,
  updated_at = now(),
  completed_at = CASE WHEN @state:text = 'completed' THEN now() ELSE NULL END
WHERE message_id = @message_id:text
  AND state = 'processing'
  AND locked_by = @worker_id:text
RETURNING message_id
'''),
        parameters: {
          'message_id': lease.messageId,
          'worker_id': lease.workerId,
          'state': state.name,
          'last_error': error,
        },
      );
      return result.isNotEmpty;
    });
  }
}
