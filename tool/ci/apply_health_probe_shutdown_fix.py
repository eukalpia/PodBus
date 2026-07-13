from pathlib import Path

source_path = Path('packages/podbus_core/lib/src/resilience.dart')
source = source_path.read_text()

old = """    final delegate = _delegate;
    if (delegate == null) {
      return;
    }
    _probeActive = true;
"""
new = """    final delegate = _delegate;
    if (delegate == null) {
      return;
    }
    final generation = _generation;
    _probeActive = true;
"""
assert source.count(old) == 2, source.count(old)
source = source.replace(old, new)

old = """      if (result.status == HealthStatus.unhealthy && !_closing) {
"""
new = """      if (result.status == HealthStatus.unhealthy &&
          !_closing &&
          generation == _generation &&
          identical(delegate, _delegate)) {
"""
assert source.count(old) == 2, source.count(old)
source = source.replace(old, new)

old = """    } on Object catch (error) {
      if (!_closing) {
        try {
          await _recover(
"""
new = """    } on Object catch (error) {
      if (!_closing &&
          generation == _generation &&
          identical(delegate, _delegate)) {
        try {
          await _recover(
"""
assert source.count(old) == 2, source.count(old)
source_path.write_text(source.replace(old, new))

test_path = Path('packages/podbus_core/test/resilience_test.dart')
tests = test_path.read_text()

replacements = [
    (
        """  _FakeMessageBus({
    this.connectGate,
    this.closeGate,
    this.connectError,
    this.subscribeError,
  });
""",
        """  _FakeMessageBus({
    this.connectGate,
    this.closeGate,
    this.healthGate,
    this.connectError,
    this.subscribeError,
  });
""",
    ),
    (
        """  final Future<void>? closeGate;
  final Object? connectError;
""",
        """  final Future<void>? closeGate;
  final Future<HealthCheckResult>? healthGate;
  final Object? connectError;
""",
    ),
    (
        "  _FakeDurableQueue({this.connectGate, this.closeGate, this.connectError});\n",
        """  _FakeDurableQueue({
    this.connectGate,
    this.closeGate,
    this.healthGate,
    this.connectError,
  });
""",
    ),
    (
        """  final Future<void>? closeGate;
  final Object? connectError;
  final List<_FakeWorker> workers = [];
""",
        """  final Future<void>? closeGate;
  final Future<HealthCheckResult>? healthGate;
  final Object? connectError;
  final List<_FakeWorker> workers = [];
""",
    ),
]
for old, new in replacements:
    assert old in tests, old[:60]
    tests = tests.replace(old, new, 1)

health_method = """  Future<HealthCheckResult> healthCheck() async {
    return connected
        ? HealthCheckResult.healthy(message: 'connected')
        : HealthCheckResult.unhealthy(message: 'disconnected');
  }
"""
health_method_new = """  Future<HealthCheckResult> healthCheck() async {
    healthChecks += 1;
    final gate = healthGate;
    if (gate != null) {
      return gate;
    }
    return connected
        ? HealthCheckResult.healthy(message: 'connected')
        : HealthCheckResult.unhealthy(message: 'disconnected');
  }
"""
assert tests.count(health_method) == 2, tests.count(health_method)
tests = tests.replace(health_method, health_method_new)
tests = tests.replace(
    '  int closedSubscriptions = 0;\n',
    '  int closedSubscriptions = 0;\n  int healthChecks = 0;\n',
    1,
)
tests = tests.replace(
    '  int closedWorkers = 0;\n',
    '  int closedWorkers = 0;\n  int healthChecks = 0;\n',
    1,
)

bus_test = """
    test('ignores a health probe completed after message-bus close', () async {
      final gate = Completer<HealthCheckResult>();
      final delegates = <_FakeMessageBus>[];
      final bus = ResilientMessageBus(
        factory: () {
          final value = _FakeMessageBus(
            healthGate: delegates.isEmpty ? gate.future : null,
          );
          delegates.add(value);
          return value;
        },
        policy: const ReconnectPolicy(
          initialDelay: Duration.zero,
          maxDelay: Duration.zero,
          jitter: 0,
          healthCheckInterval: Duration(milliseconds: 1),
          healthCheckTimeout: Duration(seconds: 1),
        ),
      );

      await bus.connect();
      await _waitUntil(() => delegates.first.healthChecks > 0);
      await bus.close(timeout: const Duration(milliseconds: 100));
      gate.complete(HealthCheckResult.unhealthy(message: 'late'));
      await Future<void>.delayed(const Duration(milliseconds: 30));

      expect(delegates, hasLength(1));
      await bus.close();
    });

"""
marker = "  });\n\n  group('ResilientDurableJobQueue', () {\n"
assert marker in tests
tests = tests.replace(marker, bus_test + marker, 1)

queue_test = """
    test('ignores a health probe completed after durable-queue close', () async {
      final gate = Completer<HealthCheckResult>();
      final delegates = <_FakeDurableQueue>[];
      final queue = ResilientDurableJobQueue(
        factory: () {
          final value = _FakeDurableQueue(
            healthGate: delegates.isEmpty ? gate.future : null,
          );
          delegates.add(value);
          return value;
        },
        policy: const ReconnectPolicy(
          initialDelay: Duration.zero,
          maxDelay: Duration.zero,
          jitter: 0,
          healthCheckInterval: Duration(milliseconds: 1),
          healthCheckTimeout: Duration(seconds: 1),
        ),
      );

      await queue.connect();
      await _waitUntil(() => delegates.first.healthChecks > 0);
      await queue.close(timeout: const Duration(milliseconds: 100));
      gate.complete(HealthCheckResult.unhealthy(message: 'late'));
      await Future<void>.delayed(const Duration(milliseconds: 30));

      expect(delegates, hasLength(1));
      await queue.close();
    });

"""
marker = "  group('ResilientDurableJobQueue', () {\n"
assert marker in tests
test_path.write_text(tests.replace(marker, marker + queue_test, 1))
