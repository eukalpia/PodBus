from pathlib import Path

source_path = Path('packages/podbus_core/lib/src/resilience.dart')
source = source_path.read_text()

# Concurrent close callers must observe one shared lifecycle operation.
source = source.replace(
    '  Future<void>? _recovery;\n  Timer? _healthTimer;',
    '  Future<void>? _recovery;\n  Future<void>? _closeFuture;\n  Timer? _healthTimer;',
)
assert source.count('  Future<void>? _closeFuture;') == 2

message_start = source.index(
    '  @override\n  Future<void> close({Duration? timeout}) async {',
    source.index('final class ResilientMessageBus'),
)
message_end = source.index(
    '\n  @override\n  Future<void> publish<T>(',
    message_start,
)
message_close = '''  @override
  Future<void> close({Duration? timeout}) {
    final existing = _closeFuture;
    if (existing != null) {
      return existing;
    }
    final effectiveTimeout = timeout ?? policy.recoveryTimeout;
    late final Future<void> closing;
    closing = _closeResources(effectiveTimeout).whenComplete(() {
      if (identical(_closeFuture, closing)) {
        _closeFuture = null;
      }
    });
    _closeFuture = closing;
    return closing;
  }

  Future<void> _closeResources(Duration effectiveTimeout) async {
    _closing = true;
    _generation += 1;
    _stopHealthMonitor();

    final registrations = _registrations.toList();
    _registrations.clear();
    final delegate = _delegate;
    _delegate = null;
    Object? failure;
    StackTrace? failureStackTrace;

    Future<void> guard(
      String component,
      Future<void> Function() action,
    ) async {
      try {
        await action().timeout(
          effectiveTimeout,
          onTimeout: () {
            throw MessagingTimeoutException(
              'Message bus $component shutdown exceeded $effectiveTimeout.',
              timeout: effectiveTimeout,
            );
          },
        );
      } on Object catch (error, stackTrace) {
        failure ??= error;
        failureStackTrace ??= stackTrace;
      }
    }

    try {
      // Registration shutdown and transport shutdown intentionally run together.
      // A worker can be blocked in a broker fetch while the transport close is
      // the operation that releases that fetch. Serial shutdown deadlocks that
      // dependency and can skip adapter cleanup after a timeout.
      await Future.wait([
        if (delegate != null)
          guard(
            'delegate',
            () => delegate.close(timeout: effectiveTimeout),
          ),
        for (final registration in registrations)
          guard(
            'subscription registration',
            () => registration.close(remove: false),
          ),
      ]);
    } finally {
      _closing = false;
    }

    if (failure != null) {
      Error.throwWithStackTrace(failure!, failureStackTrace!);
    }
  }
'''
source = source[:message_start] + message_close + source[message_end:]

queue_start = source.index(
    '  @override\n  Future<void> close({Duration? timeout}) async {',
    source.index('final class ResilientDurableJobQueue'),
)
queue_end = source.index(
    '\n  @override\n  Future<void> enqueue<T>(',
    queue_start,
)
queue_close = '''  @override
  Future<void> close({Duration? timeout}) {
    final existing = _closeFuture;
    if (existing != null) {
      return existing;
    }
    final effectiveTimeout = timeout ?? policy.recoveryTimeout;
    late final Future<void> closing;
    closing = _closeResources(effectiveTimeout).whenComplete(() {
      if (identical(_closeFuture, closing)) {
        _closeFuture = null;
      }
    });
    _closeFuture = closing;
    return closing;
  }

  Future<void> _closeResources(Duration effectiveTimeout) async {
    _closing = true;
    _generation += 1;
    _stopHealthMonitor();

    final registrations = _registrations.toList();
    _registrations.clear();
    final delegate = _delegate;
    _delegate = null;
    Object? failure;
    StackTrace? failureStackTrace;

    Future<void> guard(
      String component,
      Future<void> Function() action,
    ) async {
      try {
        await action().timeout(
          effectiveTimeout,
          onTimeout: () {
            throw MessagingTimeoutException(
              'Durable job queue $component shutdown exceeded '
              '$effectiveTimeout.',
              timeout: effectiveTimeout,
            );
          },
        );
      } on Object catch (error, stackTrace) {
        failure ??= error;
        failureStackTrace ??= stackTrace;
      }
    }

    try {
      // Close proxy workers and their owning transport concurrently. Broker
      // fetches can only unblock after the adapter closes its socket/channel.
      await Future.wait([
        if (delegate != null)
          guard(
            'delegate',
            () => delegate.close(timeout: effectiveTimeout),
          ),
        for (final registration in registrations)
          guard(
            'worker registration',
            () => registration.close(remove: false),
          ),
      ]);
    } finally {
      _closing = false;
    }

    if (failure != null) {
      Error.throwWithStackTrace(failure!, failureStackTrace!);
    }
  }
'''
source = source[:queue_start] + queue_close + source[queue_end:]
source_path.write_text(source)

test_path = Path('packages/podbus_core/test/resilience_test.dart')
tests = test_path.read_text()

# Add deterministic gates to the fake subscription/worker delegates.
tests = tests.replace(
    '''    this.healthGate,
    this.connectError,
    this.subscribeError,
  });''',
    '''    this.healthGate,
    this.subscriptionCloseGate,
    this.connectError,
    this.subscribeError,
  });''',
    1,
)
tests = tests.replace(
    '''  final Future<HealthCheckResult>? healthGate;
  final Object? connectError;''',
    '''  final Future<HealthCheckResult>? healthGate;
  final Future<void>? subscriptionCloseGate;
  final Object? connectError;''',
    1,
)
tests = tests.replace(
    '''      deliver: (payload) => handler(_FakeMessageContext(subject), payload as T),
    );''',
    '''      deliver: (payload) => handler(_FakeMessageContext(subject), payload as T),
      closeGate: subscriptionCloseGate,
    );''',
    1,
)
tests = tests.replace(
    '''    required this.deliver,
  });''',
    '''    required this.deliver,
    this.closeGate,
  });''',
    1,
)
tests = tests.replace(
    '''  final Future<void> Function(Object? payload) deliver;
  bool closed = false;''',
    '''  final Future<void> Function(Object? payload) deliver;
  final Future<void>? closeGate;
  bool closed = false;''',
    1,
)
tests = tests.replace(
    '''    closed = true;
    onClose();
  }
}

final class _FakeDurableQueue''',
    '''    closed = true;
    await closeGate;
    onClose();
  }
}

final class _FakeDurableQueue''',
    1,
)

tests = tests.replace(
    '''    this.healthGate,
    this.connectError,
  });''',
    '''    this.healthGate,
    this.workerCloseGate,
    this.connectError,
  });''',
    1,
)
tests = tests.replace(
    '''  final Future<HealthCheckResult>? healthGate;
  final Object? connectError;
  final List<_FakeWorker> workers = [];''',
    '''  final Future<HealthCheckResult>? healthGate;
  final Future<void>? workerCloseGate;
  final Object? connectError;
  final List<_FakeWorker> workers = [];''',
    1,
)
tests = tests.replace(
    '''      onClose: () => closedWorkers += 1,
    );''',
    '''      onClose: () => closedWorkers += 1,
      closeGate: workerCloseGate,
    );''',
    1,
)
tests = tests.replace(
    '''    required this.onClose,
  });

  final String topic;''',
    '''    required this.onClose,
    this.closeGate,
  });

  final String topic;''',
    1,
)
tests = tests.replace(
    '''  final void Function() onClose;
  bool closed = false;

  @override
  Future<void> close() async {
    if (closed) return;
    closed = true;
    onClose();''',
    '''  final void Function() onClose;
  final Future<void>? closeGate;
  bool closed = false;

  @override
  Future<void> close() async {
    if (closed) return;
    closed = true;
    await closeGate;
    onClose();''',
    1,
)

message_test = '''
    test(
      'closes the transport while a subscription proxy is stalled',
      () async {
        final gate = Completer<void>();
        final delegate = _FakeMessageBus(
          subscriptionCloseGate: gate.future,
        );
        final bus = ResilientMessageBus(
          factory: () => delegate,
          policy: const ReconnectPolicy(healthCheckInterval: null),
        );
        await bus.connect();
        await bus.subscribe<int>('events.stalled', handler: (_, _) async {});

        await expectLater(
          bus.close(timeout: const Duration(milliseconds: 20)),
          throwsA(isA<MessagingTimeoutException>()),
        );

        expect(delegate.closed, isTrue);
        gate.complete();
        await Future<void>.delayed(Duration.zero);
        await bus.close();
      },
    );

    test('coalesces concurrent close calls', () async {
      final gate = Completer<void>();
      final delegate = _FakeMessageBus(closeGate: gate.future);
      final bus = ResilientMessageBus(
        factory: () => delegate,
        policy: const ReconnectPolicy(healthCheckInterval: null),
      );
      await bus.connect();

      final first = bus.close(timeout: const Duration(seconds: 1));
      final second = bus.close(timeout: const Duration(seconds: 1));
      expect(identical(first, second), isTrue);
      gate.complete();
      await Future.wait([first, second]);
    });

'''
marker = "  });\n\n  group('ResilientDurableJobQueue', () {\n"
assert marker in tests
tests = tests.replace(marker, message_test + marker, 1)

queue_test = '''
    test('closes the transport while a worker proxy is stalled', () async {
      final gate = Completer<void>();
      final delegate = _FakeDurableQueue(workerCloseGate: gate.future);
      final queue = ResilientDurableJobQueue(
        factory: () => delegate,
        policy: const ReconnectPolicy(healthCheckInterval: null),
      );
      await queue.connect();
      await queue.worker<int>('jobs.stalled', handler: (_, _) async {});

      await expectLater(
        queue.close(timeout: const Duration(milliseconds: 20)),
        throwsA(isA<MessagingTimeoutException>()),
      );

      expect(delegate.closed, isTrue);
      gate.complete();
      await Future<void>.delayed(Duration.zero);
      await queue.close();
    });

'''
marker = "    test('close drains active proxy registrations and delegate', () async {\n"
assert marker in tests
tests = tests.replace(marker, queue_test + marker, 1)

test_path.write_text(tests)
