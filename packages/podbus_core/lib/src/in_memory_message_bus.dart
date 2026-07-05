import 'dart:async';
import 'dart:collection';

import 'package:uuid/uuid.dart';

import 'exceptions.dart';
import 'headers.dart';
import 'health.dart';
import 'message_bus.dart';
import 'message_context.dart';

final class InMemoryMessageBus implements MessageBus {
  InMemoryMessageBus({Uuid? uuid}) : _uuid = uuid ?? const Uuid();

  final Uuid _uuid;
  final List<_MessageSubscription> _subscriptions = [];
  final Map<String, int> _queueGroupOffsets = {};
  var _connected = false;

  @override
  Future<void> connect() async {
    _connected = true;
  }

  @override
  Future<void> close({Duration? timeout}) async {
    _connected = false;
    _subscriptions.clear();
    _queueGroupOffsets.clear();
  }

  @override
  Future<void> publish<T>(
    String subject,
    T payload, {
    MessageHeaders? headers,
  }) async {
    _ensureConnected();

    final matching = [
      for (final subscription in _subscriptions)
        if (!subscription.closed && subscription.subject == subject)
          subscription,
    ];

    final direct = matching.where(
      (subscription) => subscription.queueGroup == null,
    );
    final grouped = <String, List<_MessageSubscription>>{};
    for (final subscription in matching.where(
      (item) => item.queueGroup != null,
    )) {
      grouped.putIfAbsent(subscription.queueGroup!, () => []).add(subscription);
    }

    final deliveries = <Future<void>>[
      for (final subscription in direct)
        subscription.deliver(
          subject: subject,
          payload: payload,
          headers: headers ?? MessageHeaders(),
          bus: this,
        ),
      for (final MapEntry(:key, :value) in grouped.entries)
        _deliverToQueueGroup(
          key,
          value,
          subject: subject,
          payload: payload,
          headers: headers ?? MessageHeaders(),
        ),
    ];

    await Future.wait(deliveries);
  }

  @override
  Future<Subscription> subscribe<T>(
    String subject, {
    String? queueGroup,
    required Future<void> Function(MessageContext context, T payload) handler,
  }) async {
    _ensureConnected();

    final subscription = _InMemorySubscription<T>(
      subject: subject,
      queueGroup: queueGroup,
      handler: handler,
      onClose: (_MessageSubscription subscription) {
        _subscriptions.remove(subscription);
      },
    );
    _subscriptions.add(subscription);
    return subscription;
  }

  @override
  Future<TResponse> request<TRequest, TResponse>(
    String subject,
    TRequest payload, {
    MessageHeaders? headers,
    Duration? timeout,
  }) async {
    _ensureConnected();

    final replySubject = '_INBOX.${_uuid.v4()}';
    final completer = Completer<TResponse>();
    late final Subscription subscription;

    subscription = await subscribe<TResponse>(
      replySubject,
      handler: (_, response) async {
        if (!completer.isCompleted) {
          completer.complete(response);
        }
      },
    );

    try {
      await _publishWithReplySubject(
        subject,
        payload,
        headers: headers ?? MessageHeaders(),
        replySubject: replySubject,
      );

      final effectiveTimeout = timeout ?? Duration(seconds: 30);
      return await completer.future.timeout(
        effectiveTimeout,
        onTimeout: () {
          throw MessagingTimeoutException(
            'Request to $subject timed out after $effectiveTimeout.',
          );
        },
      );
    } finally {
      await subscription.close();
    }
  }

  @override
  Future<HealthCheckResult> healthCheck() async {
    if (!_connected) {
      return HealthCheckResult.unhealthy(
        message: 'In-memory bus is not connected.',
      );
    }
    return HealthCheckResult.healthy(
      message: 'In-memory bus is connected.',
      details: {'subscriptions': _subscriptions.length},
    );
  }

  Future<void> _publishWithReplySubject<T>(
    String subject,
    T payload, {
    required MessageHeaders headers,
    required String replySubject,
  }) async {
    final matching = [
      for (final subscription in _subscriptions)
        if (!subscription.closed && subscription.subject == subject)
          subscription,
    ];

    await Future.wait([
      for (final subscription in matching)
        subscription.deliver(
          subject: subject,
          payload: payload,
          headers: headers,
          bus: this,
          replySubject: replySubject,
        ),
    ]);
  }

  Future<void> _deliverToQueueGroup<T>(
    String queueGroup,
    List<_MessageSubscription> subscriptions, {
    required String subject,
    required T payload,
    required MessageHeaders headers,
  }) {
    final key = '$subject::$queueGroup';
    final offset = _queueGroupOffsets[key] ?? 0;
    final subscription = subscriptions[offset % subscriptions.length];
    _queueGroupOffsets[key] = offset + 1;

    return subscription.deliver(
      subject: subject,
      payload: payload,
      headers: headers,
      bus: this,
    );
  }

  void _ensureConnected() {
    if (!_connected) {
      throw const MessagingConnectionException(
        'In-memory bus is not connected.',
      );
    }
  }
}

abstract interface class _MessageSubscription implements Subscription {
  String get subject;

  String? get queueGroup;

  bool get closed;

  Future<void> deliver({
    required String subject,
    required Object? payload,
    required MessageHeaders headers,
    required InMemoryMessageBus bus,
    String? replySubject,
  });
}

final class _InMemorySubscription<T>
    implements Subscription, _MessageSubscription {
  _InMemorySubscription({
    required this.subject,
    required this.queueGroup,
    required this.handler,
    required this.onClose,
  });

  @override
  final String subject;
  @override
  final String? queueGroup;
  final Future<void> Function(MessageContext context, T payload) handler;
  final void Function(_MessageSubscription subscription) onClose;
  @override
  var closed = false;

  @override
  Future<void> deliver({
    required String subject,
    required Object? payload,
    required MessageHeaders headers,
    required InMemoryMessageBus bus,
    String? replySubject,
  }) async {
    if (closed) {
      return;
    }
    if (payload is! T) {
      throw MessageCodecException(
        'Subscriber for $subject expected $T but received ${payload.runtimeType}.',
      );
    }

    final context = _InMemoryMessageContext(
      subject: subject,
      headers: headers,
      rawMessage: payload,
      bus: bus,
      replySubject: replySubject,
    );
    await handler(context, payload);
  }

  @override
  Future<void> close() async {
    if (closed) {
      return;
    }
    closed = true;
    onClose(this);
  }
}

final class _InMemoryMessageContext implements MessageContext {
  _InMemoryMessageContext({
    required this.subject,
    required this.headers,
    required this.rawMessage,
    required this._bus,
    required this._replySubject,
  });

  final InMemoryMessageBus _bus;
  final String? _replySubject;
  final Queue<String> _state = Queue();

  @override
  final String subject;

  @override
  final MessageHeaders headers;

  @override
  final Object? rawMessage;

  @override
  Future<void> ack() async {
    _state.add('ack');
  }

  @override
  Future<void> extendVisibility(Duration duration) async {
    _state.add('extendVisibility');
  }

  @override
  Future<void> nak({Duration? delay}) async {
    _state.add('nak');
  }

  @override
  Future<void> reply<T>(T payload, {MessageHeaders? headers}) async {
    final replySubject = _replySubject;
    if (replySubject == null) {
      throw const MessagingUnsupportedException(
        'This message does not have a reply subject.',
      );
    }
    await _bus.publish(replySubject, payload, headers: headers);
  }

  @override
  Future<void> terminate() async {
    _state.add('terminate');
  }
}
