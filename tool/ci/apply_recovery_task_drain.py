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
