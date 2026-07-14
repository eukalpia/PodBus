from pathlib import Path

path = Path('packages/podbus_core/lib/src/resilience.dart')
source = path.read_text()

marker = '''    _stopHealthMonitor();

    final registrations = _registrations.toList();'''
replacement = '''    _stopHealthMonitor();

    final recovery = _recovery;
    final registrations = _registrations.toList();'''
if source.count(marker) != 2:
    raise SystemExit(f'expected 2 close markers, found {source.count(marker)}')
source = source.replace(marker, replacement)

message_old = '''    try {
      // Registration shutdown and transport shutdown intentionally run together.
      // A worker can be blocked in a broker fetch while the transport close is
      // the operation that releases that fetch. Serial shutdown deadlocks that
      // dependency and can skip adapter cleanup after a timeout.
      await Future.wait([
        if (delegate != null)
          guard('delegate', () => delegate.close(timeout: effectiveTimeout)),
        for (final registration in registrations)
          guard(
            'subscription registration',
            () => registration.close(remove: false),
          ),
      ]);
    } finally {
      _closing = false;
    }
'''
message_new = '''    try {
      await Future.wait([
        if (delegate != null)
          guard('delegate', () => delegate.close(timeout: effectiveTimeout)),
        for (final registration in registrations)
          guard(
            'subscription registration',
            () => registration.close(remove: false),
          ),
        if (recovery != null)
          guard('recovery task', () async {
            try {
              await recovery;
            } on Object {
              // Shutdown invalidates an in-flight recovery attempt.
            }
          }),
      ]);

      final lateDelegate = _delegate;
      _delegate = null;
      if (lateDelegate != null && !identical(lateDelegate, delegate)) {
        await guard(
          'late delegate',
          () => lateDelegate.close(timeout: effectiveTimeout),
        );
      }
    } finally {
      _closing = false;
    }
'''
if message_old not in source:
    raise SystemExit('message bus close block not found')
source = source.replace(message_old, message_new, 1)

durable_old = '''    try {
      // Close proxy workers and their owning transport concurrently. Broker
      // fetches can only unblock after the adapter closes its socket/channel.
      await Future.wait([
        if (delegate != null)
          guard('delegate', () => delegate.close(timeout: effectiveTimeout)),
        for (final registration in registrations)
          guard('worker registration', () => registration.close(remove: false)),
      ]);
    } finally {
      _closing = false;
    }
'''
durable_new = '''    try {
      await Future.wait([
        if (delegate != null)
          guard('delegate', () => delegate.close(timeout: effectiveTimeout)),
        for (final registration in registrations)
          guard('worker registration', () => registration.close(remove: false)),
        if (recovery != null)
          guard('recovery task', () async {
            try {
              await recovery;
            } on Object {
              // Shutdown invalidates an in-flight recovery attempt.
            }
          }),
      ]);

      final lateDelegate = _delegate;
      _delegate = null;
      if (lateDelegate != null && !identical(lateDelegate, delegate)) {
        await guard(
          'late delegate',
          () => lateDelegate.close(timeout: effectiveTimeout),
        );
      }
    } finally {
      _closing = false;
    }
'''
if durable_old not in source:
    raise SystemExit('durable queue close block not found')
source = source.replace(durable_old, durable_new, 1)

retry_marker = '''            lastError = error;
            lastStackTrace = stackTrace;
            _lastRecoveryError = error;'''
retry_replacement = '''            if (_closing || generation != _generation) {
              Error.throwWithStackTrace(error, stackTrace);
            }
            lastError = error;
            lastStackTrace = stackTrace;
            _lastRecoveryError = error;'''
if source.count(retry_marker) != 2:
    raise SystemExit(
        f'expected 2 recovery catch markers, found {source.count(retry_marker)}'
    )
source = source.replace(retry_marker, retry_replacement)
path.write_text(source)

adapter = Path('packages/podbus_rabbitmq/lib/src/rabbitmq_adapter.dart')
source = adapter.read_text()

field_marker = '''  var _publisherConfirmTimeout = const Duration(seconds: 5);
  var _mandatoryPublish = true;'''
field_replacement = '''  var _publisherConfirmTimeout = const Duration(seconds: 5);
  var _closeTimeout = const Duration(seconds: 5);
  var _mandatoryPublish = true;'''
if field_marker not in source:
    raise SystemExit('RabbitMQ close-timeout field marker not found')
source = source.replace(field_marker, field_replacement, 1)

connect_marker = '''    _publisherConfirmTimeout = config.publisherConfirmTimeout;
    _mandatoryPublish = config.mandatoryPublish;'''
connect_replacement = '''    _publisherConfirmTimeout = config.publisherConfirmTimeout;
    _closeTimeout = config.connectTimeout;
    _mandatoryPublish = config.mandatoryPublish;'''
if connect_marker not in source:
    raise SystemExit('RabbitMQ close-timeout connect marker not found')
source = source.replace(connect_marker, connect_replacement, 1)

close_old = '''  @override
  Future<void> close() async {
    _connected = false;
    _publishEpoch += 1;
    _failPendingPublishes(
      const MessagingConnectionException(
        'RabbitMQ adapter closed before publisher confirmation.',
      ),
    );

    await _clientErrors?.cancel();
    _clientErrors = null;
    for (final lane in _publisherLanes) {
      await lane.close();
    }
    _publisherLanes.clear();
    await _consumerChannel?.close();
    await _client?.close();
    _consumerChannel = null;
    _client = null;
    _consumerExchanges.clear();
    _queues.clear();
  }
'''
close_new = '''  @override
  Future<void> close() async {
    _connected = false;
    _publishEpoch += 1;
    _failPendingPublishes(
      const MessagingConnectionException(
        'RabbitMQ adapter closed before publisher confirmation.',
      ),
    );

    final clientErrors = _clientErrors;
    final lanes = _publisherLanes.toList();
    final consumerChannel = _consumerChannel;
    final client = _client;
    _clientErrors = null;
    _publisherLanes.clear();
    _consumerChannel = null;
    _client = null;
    _consumerExchanges.clear();
    _queues.clear();

    Object? failure;
    StackTrace? failureStackTrace;

    Future<void> guard(String component, Future<void> Function() action) async {
      try {
        await action().timeout(
          _closeTimeout,
          onTimeout: () => throw MessagingTimeoutException(
            'RabbitMQ adapter $component close exceeded $_closeTimeout.',
            timeout: _closeTimeout,
          ),
        );
      } on Object catch (error, stackTrace) {
        failure ??= error;
        failureStackTrace ??= stackTrace;
      }
    }

    await Future.wait([
      if (clientErrors != null)
        guard('error subscription', clientErrors.cancel),
      if (client != null) guard('client', client.close),
      if (consumerChannel != null)
        guard('consumer channel', consumerChannel.close),
      for (final lane in lanes) guard('publisher lane', lane.close),
    ]);

    if (failure != null) {
      Error.throwWithStackTrace(failure!, failureStackTrace!);
    }
  }
'''
if close_old not in source:
    raise SystemExit('RabbitMQ adapter close block not found')
source = source.replace(close_old, close_new, 1)
adapter.write_text(source)

suite = Path('tool/fault_suite.dart')
source = suite.read_text()
finally_old = '''  } finally {
    await queue.close(timeout: const Duration(seconds: 5)).catchError((_) {});
  }
}

Future<Map<String, Object?>> _natsCrashBeforeAck'''
finally_new = '''  } finally {
    await queue.close(timeout: const Duration(seconds: 5)).catchError((_) {});
    await Future.wait([
      for (final adapter in adapters)
        adapter.close().timeout(const Duration(seconds: 5)).catchError((_) {}),
    ]);
  }
}

Future<Map<String, Object?>> _natsCrashBeforeAck'''
if finally_old not in source:
    raise SystemExit('RabbitMQ channel-failure cleanup block not found')
source = source.replace(finally_old, finally_new, 1)
suite.write_text(source)
