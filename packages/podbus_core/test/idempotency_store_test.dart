import 'package:podbus_core/podbus_core.dart';
import 'package:test/test.dart';

void main() {
  group('InMemoryIdempotencyStore', () {
    test('claims a key once within its ttl', () async {
      final store = InMemoryIdempotencyStore();

      expect(
        await store.claim('welcome-email:1', ttl: Duration(minutes: 5)),
        isTrue,
      );
      expect(
        await store.claim('welcome-email:1', ttl: Duration(minutes: 5)),
        isFalse,
      );
    });

    test('allows a key after the ttl expires', () async {
      final now = DateTime.utc(2026, 1, 1);
      final store = InMemoryIdempotencyStore(clock: () => now);

      expect(await store.claim('job:1', ttl: Duration.zero), isTrue);
      expect(await store.claim('job:1', ttl: Duration.zero), isTrue);
    });
  });
}
