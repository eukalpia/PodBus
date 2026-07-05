import 'package:podbus_core/podbus_core.dart';
import 'package:test/test.dart';

void main() {
  group('MessageHeaders', () {
    test('serializes standard and custom headers without dropping values', () {
      final headers = MessageHeaders(
        correlationId: 'corr-1',
        causationId: 'cause-1',
        tenantId: 'tenant-1',
        userId: 'user-1',
        traceId: 'trace-1',
        idempotencyKey: 'idem-1',
        attempt: 2,
        custom: {'x-region': 'eu', 'x-priority': 'high'},
      );

      final decoded = MessageHeaders.fromMap(headers.toMap());

      expect(decoded.correlationId, 'corr-1');
      expect(decoded.causationId, 'cause-1');
      expect(decoded.tenantId, 'tenant-1');
      expect(decoded.userId, 'user-1');
      expect(decoded.traceId, 'trace-1');
      expect(decoded.idempotencyKey, 'idem-1');
      expect(decoded.attempt, 2);
      expect(decoded.custom, {'x-region': 'eu', 'x-priority': 'high'});
    });

    test('copyWith preserves custom headers and can increment attempt', () {
      final headers = MessageHeaders(
        correlationId: 'corr-1',
        attempt: 1,
        custom: {'x-region': 'eu'},
      );

      final next = headers.incrementAttempt();

      expect(next.correlationId, 'corr-1');
      expect(next.attempt, 2);
      expect(next.custom, {'x-region': 'eu'});
    });

    test('rejects reserved custom header names', () {
      expect(
        () => MessageHeaders(custom: {'correlationId': 'shadowed'}),
        throwsA(isA<MessagingConfigurationException>()),
      );
    });
  });
}
