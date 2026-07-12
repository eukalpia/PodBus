import 'dart:async';

import 'package:podbus_core/podbus_core.dart';

import 'nats_jetstream_adapter.dart';

/// Runs [action] while periodically extending the JetStream acknowledgement
/// window for the current job.
///
/// Use this for handlers that may run longer than the consumer acknowledgement
/// wait. The helper is transport-aware: it requires a JetStream-backed
/// [JobContext] and fails fast for other transports.
Future<T> runWithNatsJetStreamHeartbeat<T>(
  JobContext context,
  Future<T> Function() action, {
  Duration interval = const Duration(seconds: 20),
  void Function(Object error, StackTrace stackTrace)? onHeartbeatError,
}) async {
  if (interval <= Duration.zero) {
    return action();
  }

  final rawMessage = context.rawMessage;
  if (rawMessage is! NatsJetStreamMessage) {
    throw const MessagingUnsupportedException(
      'JetStream heartbeat requires a NATS JetStream job context.',
    );
  }

  final heartbeat = _Heartbeat(
    message: rawMessage,
    interval: interval,
    onError: onHeartbeatError,
  )..start();
  try {
    return await action();
  } finally {
    await heartbeat.stop();
  }
}

final class _Heartbeat {
  _Heartbeat({
    required this.message,
    required this.interval,
    required this.onError,
  });

  final NatsJetStreamMessage message;
  final Duration interval;
  final void Function(Object error, StackTrace stackTrace)? onError;
  Timer? _timer;
  Future<void>? _active;
  var _stopped = false;
  Object? _lastError;
  StackTrace? _lastStackTrace;

  void start() {
    _timer = Timer.periodic(interval, (_) => _tick());
  }

  void _tick() {
    if (_stopped || _active != null) {
      return;
    }
    late final Future<void> task;
    task =
        Future<void>.sync(() async {
              final accepted = await message.inProgress();
              if (!accepted) {
                throw const MessagingConnectionException(
                  'JetStream rejected the in-progress heartbeat.',
                );
              }
            })
            .then<void>(
              (_) {},
              onError: (Object error, StackTrace stackTrace) {
                _lastError = error;
                _lastStackTrace = stackTrace;
                onError?.call(error, stackTrace);
              },
            )
            .whenComplete(() {
              if (identical(_active, task)) {
                _active = null;
              }
            });
    _active = task;
  }

  Future<void> stop() async {
    _stopped = true;
    _timer?.cancel();
    final active = _active;
    if (active != null) {
      await active;
    }
    final error = _lastError;
    if (error != null && onError == null) {
      Error.throwWithStackTrace(error, _lastStackTrace ?? StackTrace.current);
    }
  }
}
