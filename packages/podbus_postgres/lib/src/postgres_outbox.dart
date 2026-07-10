import 'dart:convert';
import 'dart:typed_data';

import 'package:podbus_core/podbus_core.dart';
import 'package:postgres/postgres.dart';
import 'package:uuid/uuid.dart';

import 'postgres_schema.dart';

enum OutboxState { pending, processing, failed, published, dead }

final class OutboxRecord {
  const OutboxRecord({
    required this.id,
    required this.topic,
    required this.key,
    required this.encoded,
    required this.headers,
    required this.attempts,
  });

  final String id;
  final String topic;
  final String? key;
  final EncodedMessage encoded;
  final MessageHeaders headers;
  final int attempts;
}

final class PostgresOutbox {
  PostgresOutbox(
    this.executor, {
    PostgresMessagingSchema? schema,
    MessageCodec? codec,
    Uuid? uuid,
    this.maxErrorCharacters = 4096,
  }) : schema = schema ?? PostgresMessagingSchema(),
       codec = codec ?? const JsonMessageCodec(),
       _uuid = uuid ?? Uuid() {
    if (maxErrorCharacters < 1) {
      throw const MessagingConfigurationException(
        'Outbox maxErrorCharacters must be greater than zero.',
      );
    }
  }

  final SessionExecutor executor;
  final PostgresMessagingSchema schema;
  final MessageCodec codec;
  final Uuid _uuid;
  final int maxErrorCharacters;

  Future<void> install() => executor.run(schema.install);

  Future<R> runTransaction<R>(
    Future<R> Function(TxSession transaction) action,
  ) {
    return executor.runTx(action);
  }

  Future<String> enqueue<T>(
    Session session,
    String topic,
    T payload, {
    String? id,
    String? key,
    MessageHeaders? headers,
    DateTime? availableAt,
  }) async {
    if (topic.trim().isEmpty) {
      throw const MessagingConfigurationException(
        'Outbox topic must not be empty.',
      );
    }
    final encoded = await codec.encode(payload);
    final messageId = id ?? _uuid.v4();
    final messageHeaders = headers ?? MessageHeaders();
    await session.execute(
      Sql.named('''
INSERT INTO ${schema.outboxTable} (
  id,
  topic,
  message_key,
  payload,
  content_type,
  schema_version,
  message_type,
  headers,
  next_attempt_at
)
VALUES (
  @id:text,
  @topic:text,
  @message_key:text,
  @payload:bytea,
  @content_type:text,
  @schema_version:int4,
  @message_type:text,
  CAST(@headers:text AS jsonb),
  @next_attempt_at:timestamptz
)
'''),
      parameters: {
        'id': messageId,
        'topic': topic,
        'message_key': key,
        'payload': encoded.bytes,
        'content_type': encoded.contentType,
        'schema_version': encoded.schemaVersion,
        'message_type': encoded.messageType,
        'headers': jsonEncode(messageHeaders.toMap()),
        'next_attempt_at': availableAt ?? DateTime.now().toUtc(),
      },
      ignoreRows: true,
    );
    return messageId;
  }

  Future<List<OutboxRecord>> claimBatch({
    required String workerId,
    int batchSize = 100,
    Duration lease = const Duration(minutes: 1),
  }) async {
    if (workerId.trim().isEmpty) {
      throw const MessagingConfigurationException(
        'Outbox workerId must not be empty.',
      );
    }
    if (batchSize < 1 || lease <= Duration.zero) {
      throw const MessagingConfigurationException(
        'Outbox batchSize and lease must be greater than zero.',
      );
    }

    return executor.runTx((session) async {
      final result = await session.execute(
        Sql.named('''
WITH candidates AS (
  SELECT id
  FROM ${schema.outboxTable}
  WHERE status IN ('pending', 'failed')
    AND next_attempt_at <= now()
    AND (locked_until IS NULL OR locked_until <= now())
  ORDER BY created_at, id
  FOR UPDATE SKIP LOCKED
  LIMIT @batch_size:int4
)
UPDATE ${schema.outboxTable} AS outbox
SET
  status = 'processing',
  attempts = outbox.attempts + 1,
  locked_by = @worker_id:text,
  locked_until = now() + (@lease_ms:int8 * interval '1 millisecond'),
  last_error = NULL
FROM candidates
WHERE outbox.id = candidates.id
RETURNING
  outbox.id,
  outbox.topic,
  outbox.message_key,
  outbox.payload,
  outbox.content_type,
  outbox.schema_version,
  outbox.message_type,
  outbox.headers,
  outbox.attempts
'''),
        parameters: {
          'worker_id': workerId,
          'batch_size': batchSize,
          'lease_ms': lease.inMilliseconds,
        },
      );
      return [for (final row in result) _recordFromRow(row.toColumnMap())];
    });
  }

  Future<void> markPublished(String id, {required String workerId}) async {
    await _ownedUpdate(
      id,
      workerId: workerId,
      sql: '''
SET
  status = 'published',
  published_at = now(),
  locked_by = NULL,
  locked_until = NULL,
  last_error = NULL
''',
    );
  }

  Future<void> markFailed(
    String id, {
    required String workerId,
    required Object error,
    required Duration retryAfter,
    required int maxAttempts,
  }) async {
    if (retryAfter.isNegative || maxAttempts < 1) {
      throw const MessagingConfigurationException(
        'Outbox retryAfter must not be negative and maxAttempts must be positive.',
      );
    }
    final truncatedError = _truncate(error.toString());
    final updated = await executor.run((session) async {
      final result = await session.execute(
        Sql.named('''
UPDATE ${schema.outboxTable}
SET
  status = CASE WHEN attempts >= @max_attempts:int4 THEN 'dead' ELSE 'failed' END,
  next_attempt_at = now() + (@retry_ms:int8 * interval '1 millisecond'),
  locked_by = NULL,
  locked_until = NULL,
  last_error = @last_error:text
WHERE id = @id:text
  AND status = 'processing'
  AND locked_by = @worker_id:text
RETURNING id
'''),
        parameters: {
          'id': id,
          'worker_id': workerId,
          'max_attempts': maxAttempts,
          'retry_ms': retryAfter.inMilliseconds,
          'last_error': truncatedError,
        },
      );
      return result.isNotEmpty;
    });
    if (!updated) {
      throw const MessagingConnectionException(
        'Outbox record could not be failed because ownership was lost.',
      );
    }
  }

  Future<int> releaseExpiredLeases() async {
    return executor.run((session) async {
      final result = await session.execute('''
UPDATE ${schema.outboxTable}
SET
  status = 'failed',
  locked_by = NULL,
  locked_until = NULL,
  next_attempt_at = now(),
  last_error = COALESCE(last_error, 'worker lease expired')
WHERE status = 'processing'
  AND locked_until <= now()
RETURNING id
''');
      return result.length;
    });
  }

  Future<void> _ownedUpdate(
    String id, {
    required String workerId,
    required String sql,
  }) async {
    final updated = await executor.run((session) async {
      final result = await session.execute(
        Sql.named('''
UPDATE ${schema.outboxTable}
$sql
WHERE id = @id:text
  AND status = 'processing'
  AND locked_by = @worker_id:text
RETURNING id
'''),
        parameters: {'id': id, 'worker_id': workerId},
      );
      return result.isNotEmpty;
    });
    if (!updated) {
      throw const MessagingConnectionException(
        'Outbox record could not be updated because ownership was lost.',
      );
    }
  }

  OutboxRecord _recordFromRow(Map<String, Object?> row) {
    final rawPayload = row['payload'];
    final payload = switch (rawPayload) {
      final Uint8List bytes => bytes,
      final List<int> bytes => Uint8List.fromList(bytes),
      _ => throw MessageCodecException(
        'PostgreSQL outbox payload must be bytea, got ${rawPayload.runtimeType}.',
      ),
    };
    final rawHeaders = row['headers'];
    final headerMap = switch (rawHeaders) {
      final Map<Object?, Object?> map => <String, Object?>{
        for (final entry in map.entries)
          if (entry.key is String) entry.key as String: entry.value,
      },
      final String value => jsonDecode(value) as Map<String, Object?>,
      _ => const <String, Object?>{},
    };
    return OutboxRecord(
      id: row['id']! as String,
      topic: row['topic']! as String,
      key: row['message_key'] as String?,
      encoded: EncodedMessage(
        bytes: payload,
        contentType: row['content_type']! as String,
        schemaVersion: row['schema_version']! as int,
        messageType: row['message_type'] as String?,
      ),
      headers: MessageHeaders.fromMap(headerMap),
      attempts: row['attempts']! as int,
    );
  }

  String _truncate(String value) {
    if (value.length <= maxErrorCharacters) {
      return value;
    }
    return '${value.substring(0, maxErrorCharacters)}…';
  }
}

final class PostgresOutboxRelay {
  PostgresOutboxRelay({
    required this.outbox,
    required this.bus,
    required this.workerId,
    this.batchSize = 100,
    this.lease = const Duration(minutes: 1),
    RetryPolicy? retryPolicy,
  }) : retryPolicy =
           retryPolicy ??
           RetryPolicy(
             maxAttempts: 10,
             initialDelay: const Duration(seconds: 1),
             maxDelay: const Duration(minutes: 5),
             jitter: 0.2,
           ) {
    if (batchSize < 1 || workerId.trim().isEmpty) {
      throw const MessagingConfigurationException(
        'Outbox relay workerId must not be empty and batchSize must be positive.',
      );
    }
  }

  final PostgresOutbox outbox;
  final MessageBus bus;
  final String workerId;
  final int batchSize;
  final Duration lease;
  final RetryPolicy retryPolicy;

  Future<int> runOnce() async {
    final records = await outbox.claimBatch(
      workerId: workerId,
      batchSize: batchSize,
      lease: lease,
    );
    for (final record in records) {
      try {
        final payload = await outbox.codec.decode<Object?>(record.encoded);
        await bus.publish<Object?>(record.topic, payload, headers: record.headers);
        await outbox.markPublished(record.id, workerId: workerId);
      } on Object catch (error) {
        await outbox.markFailed(
          record.id,
          workerId: workerId,
          error: error,
          retryAfter: retryPolicy.delayForAttempt(record.attempts),
          maxAttempts: retryPolicy.maxAttempts,
        );
      }
    }
    return records.length;
  }
}
