import 'package:podbus_core/podbus_core.dart';
import 'package:postgres/postgres.dart';

import 'postgres_schema.dart';

final class PostgresIdempotencyStore implements IdempotencyStore {
  PostgresIdempotencyStore(this.executor, {PostgresMessagingSchema? schema})
    : schema = schema ?? PostgresMessagingSchema();

  final SessionExecutor executor;
  final PostgresMessagingSchema schema;

  Future<void> install() => executor.run(schema.install);

  @override
  Future<bool> claim(String key, {required Duration ttl}) async {
    if (key.trim().isEmpty) {
      throw const MessagingConfigurationException(
        'Idempotency key must not be empty.',
      );
    }
    if (ttl <= Duration.zero) {
      return true;
    }

    return executor.run((session) async {
      final result = await session.execute(
        Sql.named('''
INSERT INTO ${schema.idempotencyTable} (
  idempotency_key,
  expires_at
)
VALUES (
  @key:text,
  now() + (@ttl_ms:int8 * interval '1 millisecond')
)
ON CONFLICT (idempotency_key) DO UPDATE
SET
  expires_at = EXCLUDED.expires_at,
  created_at = now()
WHERE ${schema.idempotencyTable}.expires_at <= now()
RETURNING idempotency_key
'''),
        parameters: {'key': key, 'ttl_ms': ttl.inMilliseconds},
      );
      return result.isNotEmpty;
    });
  }

  @override
  Future<void> release(String key) async {
    await executor.run((session) async {
      await session.execute(
        Sql.named('''
DELETE FROM ${schema.idempotencyTable}
WHERE idempotency_key = @key:text
'''),
        parameters: {'key': key},
        ignoreRows: true,
      );
    });
  }

  Future<int> pruneExpired({int batchSize = 1000}) async {
    if (batchSize < 1) {
      throw const MessagingConfigurationException(
        'Idempotency prune batch size must be greater than zero.',
      );
    }
    return executor.run((session) async {
      final result = await session.execute(
        Sql.named('''
WITH expired AS (
  SELECT idempotency_key
  FROM ${schema.idempotencyTable}
  WHERE expires_at <= now()
  ORDER BY expires_at
  LIMIT @batch_size:int4
)
DELETE FROM ${schema.idempotencyTable} AS store
USING expired
WHERE store.idempotency_key = expired.idempotency_key
RETURNING store.idempotency_key
'''),
        parameters: {'batch_size': batchSize},
      );
      return result.length;
    });
  }

  @override
  Future<void> clear() async {
    await executor.run((session) async {
      await session.execute(
        'TRUNCATE TABLE ${schema.idempotencyTable}',
        ignoreRows: true,
      );
    });
  }
}
