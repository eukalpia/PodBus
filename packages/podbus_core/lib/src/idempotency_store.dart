abstract interface class IdempotencyStore {
  Future<bool> claim(String key, {required Duration ttl});

  Future<void> release(String key);

  Future<void> clear();
}

final class InMemoryIdempotencyStore implements IdempotencyStore {
  InMemoryIdempotencyStore({DateTime Function()? clock})
    : _clock = clock ?? DateTime.now;

  final DateTime Function() _clock;
  final Map<String, DateTime> _expiresAtByKey = {};

  @override
  Future<bool> claim(String key, {required Duration ttl}) async {
    _pruneExpired();

    if (ttl <= Duration.zero) {
      return true;
    }

    final now = _clock();
    final expiresAt = _expiresAtByKey[key];
    if (expiresAt != null && expiresAt.isAfter(now)) {
      return false;
    }

    _expiresAtByKey[key] = now.add(ttl);
    return true;
  }

  @override
  Future<void> release(String key) async {
    _expiresAtByKey.remove(key);
  }

  @override
  Future<void> clear() async {
    _expiresAtByKey.clear();
  }

  void _pruneExpired() {
    final now = _clock();
    _expiresAtByKey.removeWhere((_, expiresAt) => !expiresAt.isAfter(now));
  }
}
