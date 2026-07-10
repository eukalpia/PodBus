import 'dart:async';
import 'dart:collection';

import 'package:uuid/uuid.dart';

import 'capabilities.dart';
import 'config.dart';
import 'exceptions.dart';
import 'headers.dart';
import 'health.dart';
import 'message_bus.dart';
import 'message_context.dart';

final class InMemoryMessageBus implements MessageBus {
  InMemoryMessageBus({
    MessagingConfig? messagingConfig,
    Uuid? uuid,
  }) : messagingConfig = messagingConfig ?? MessagingConfig(),
       _uuid = uuid ?? const Uuid();

  static const _capabilities = MessagingCapabilities({
    MessagingCapability.publishSubscribe,
    MessagingCapability.queueGroups,
    MessagingCapability.requestReply,
    MessagingCapability.manualAcknowledgement,
    MessagingCapability.negativeAcknowledgement,
    MessagingCapability.termination,
    MessagingCapability.typedPayloads,
    MessagingCapability.gracefulShutdown,
  });

  final MessagingConfig messagingConfig;
  final Uuid _uuid;
  final List<_MessageSubscription> _subscriptions = [];
  final Map<String, int> _queueGroupOffsets = {};
  final Set<Future<void>> _activeDeliveries = {};
  var _connected = false;
  var _closing = false;

  @override
  MessagingCapabilities get capabilities => _capabilities;

  @override
  Future<void> connect() async {
    _closing = false;
    _connected = true;
    messagingConfig.log(
      MessagingLogLevel.info,
      'In-memory message bus connected.',
    );
  }

  @override
  Future<void> close({Duration? timeout}) async {
    if (!_connected && !_closing) {
      return;
    }

    _closing = true;
    _connected = false;
    final effectiveTimeout = timeout ?? messagingConfig.shutdownTimeout;
    final subscriptions = _subscriptions.toList();
    for (final subscription in subscriptions) {
      await subscription.close();
    }

    final active = _activeDeliveries.toList();
    if (active.isNotEmpty) {
      await Future.wait(active).timeout(
        effectiveTimeout,
        onTimeout: () {
          throw MessagingTimeoutException(
            'In-memory message bus did not drain within $effectiveTimeout.',
          );
        },
      );
    }

    _subscriptions.clear();
    _queueGroupOffsets.clear();
    _closing = false;
    messagingConfig.log(
      MessagingLogLevel.info,
      'In-memory message bus closed.',
    );
  }

  @override
  Future<void> publish<T>(
    String subject,
    T payload, {
    MessageHeaders? headers,
  }) async {
    _ensureConnected();
    final startedAt = messagingConfig.now();
    final effectiveHeaders = headers ?? MessageHeaders();
    messagingConfig.limits.validateHeaders(effectiveHeaders.toMap());

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
        _trackDelivery(
          subscription.deliver(
            subject: subject,
            payload: payload,
            headers: effectiveHeaders,
            bus: this,
          ),
        ),
      for (final MapEntry(:key, :value) in grouped.entries)
        _trackDelivery(
          _deliverToQueueGroup(
            key,
            value,
            subject: subject,
            payload: payload,
            headers: effectiveHeaders,
          ),
        ),
    ];

    await Future.wait(deliveries);
    messagingConfig.recordMetric(
      'podbus.messages.published',
      attributes: {'transport': 'memory', 'subject': subject},
    );
    messagingConfig.recordDuration(
      'podbus.publish.duration',
      messagingConfig.now().difference(startedAt),
      attributes: {'transport': 'memory', 'subject': subject},
    );
  }

  @override
  Future<Subscription> subscribe<T>(
    String subject, {
    String? queueGroup,
    int concurrency = 1,
    required Future<void> Function(MessageContext context, T payload) handler,
  }) async {
    _ensureConnected();
    if (concurrency < 1) {
      throw const MessagingConfigurationException(
        'Subscription concurrency must be greater than zero.',
      );
    }

    final subscription = _InMemorySubscription<T>(
      subject: subject,
      queueGroup: queueGroup,
      concurrency: concurrency,
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

      final effectiveTimeout = timeout ?? messagingConfig.requestTimeout;
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
        message: _closing
            ? 'In-memory bus is draining.'
            : 'In-memory bus is not connected.',
        details: {'activeDeliveries': _activeDeliveries.length},
      );
    }
    return HealthCheckResult.healthy(
      message: 'In-memory bus is connected.',
      details: {
        'subscriptions': _subscriptions.length,
        'activeDeliveries': _activeDeliveries.length,
      },
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
        _trackDelivery(
          subscription.deliver(
            subject: subject,
            payload: payload,
            headers: headers,
            bus: this,
            replySubject: replySubject,
          ),
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

  Future<void> _trackDelivery(Future<void> delivery) async {
    _activeDeliveries.add(delivery);
    try {
      await delivery;
    } finally {
      _activeDeliveries.remove(delivery);
    }
  }

  void _ensureConnected() {
    if (!_connected || _closing) {
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
    required int concurrency,
    required this.handler,
    required this.onClose,
  }) : _gate = _ConcurrencyGate(concurrency);

  @override
  final String subject;
  @override
  final String? queueGroup;
  final Future<void> Function(MessageContext context, T payload) handler;
  final void Function(_MessageSubscription subscription) onClose;
  final _ConcurrencyGate _gate;
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

    await _gate.run(() async {
      if (closed) {
        return;
      }
      final context = _InMemoryMessageContext(
        subject: subject,
        headers: headers,
        rawMessage: payload,
        bus: bus,
        replySubject: replySubject,
      );
      await handler(context, payload);
    });
  }

  @override
  Future<void> close() async {
    if (closed) {
      return;
    }
    closed = true;
    onClose(this);
    await _gate.drain();
  }
}

final class _ConcurrencyGate {
  _ConcurrencyGate(this.limit);

  final int limit;
  final Queue<Completer<void>> _waiters = Queue();
  final Set<Future<void>> _active = {};
  var _running = 0;

  Future<void> run(Future<void> Function() action) async {
    await _acquire();
    late final Future<void> future;
    future = Future<void>.sync(action).whenComplete(() {
      _active.remove(future);
      _release();
    });
    _active.add(future);
    return future;
  }

  Future<void> drain() async {
    while (_active.isNotEmpty) {
      await Future.wait(_active.toList());
    }
  }

  Future<void> _acquire() async {
    if (_running < limit) {
      _running++;
      return;
    }
    final waiter = Completer<void>();
    _waiters.add(waiter);
    await waiter.future;
    _running++;
  }

  void _release() {
    _running--;
    if (_waiters.isNotEmpty) {
      _waiters.removeFirst().complete();
    }
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
