import 'dart:async';

import 'package:podbus_core/podbus_core.dart';

import 'config.dart';
import 'nats_client_adapter.dart';

final class NatsMessageBus implements MessageBus {
  NatsMessageBus({
    required this.config,
    MessagingConfig? messagingConfig,
    NatsClientAdapter? clientAdapter,
    MessageCodec? codec,
  }) : messagingConfig = messagingConfig ?? MessagingConfig(codec: codec),
       _client = clientAdapter ?? DartNatsClientAdapter();

  static const _capabilities = MessagingCapabilities({
    MessagingCapability.publishSubscribe,
    MessagingCapability.queueGroups,
    MessagingCapability.requestReply,
    MessagingCapability.typedPayloads,
    MessagingCapability.gracefulShutdown,
  });

  final NatsMessagingConfig config;
  final MessagingConfig messagingConfig;
  final NatsClientAdapter _client;
  final List<_NatsSubscription> _subscriptions = [];
  var _closing = false;

  MessageCodec get _codec => messagingConfig.codec;

  @override
  MessagingCapabilities get capabilities => _capabilities;

  @override
  Future<void> connect() async {
    _closing = false;
    await _client.connect(config);
    messagingConfig.log(
      MessagingLogLevel.info,
      'NATS message bus connected.',
      attributes: {'transport': 'nats'},
    );
  }

  @override
  Future<void> close({Duration? timeout}) async {
    if (_closing) {
      return;
    }
    _closing = true;
    final effectiveTimeout = timeout ?? messagingConfig.shutdownTimeout;
    Object? failure;
    StackTrace? failureStackTrace;

    try {
      await Future.wait([
        for (final subscription in _subscriptions.toList())
          subscription.close(),
      ]).timeout(effectiveTimeout);
      _subscriptions.clear();
      await _client.drain().timeout(effectiveTimeout);
    } on Object catch (error, stackTrace) {
      failure = error;
      failureStackTrace = stackTrace;
    } finally {
      try {
        await _client.close();
      } on Object catch (error, stackTrace) {
        failure ??= error;
        failureStackTrace ??= stackTrace;
      }
      _closing = false;
    }

    if (failure != null) {
      Error.throwWithStackTrace(failure, failureStackTrace!);
    }
  }

  @override
  Future<void> publish<T>(
    String subject,
    T payload, {
    MessageHeaders? headers,
  }) async {
    _ensureAvailable();
    final startedAt = messagingConfig.now();
    final encoded = await _codec.encode(payload);
    final wireHeaders = _headersFor(headers ?? MessageHeaders(), encoded);
    messagingConfig.validateRawOutbound(encoded.bytes, wireHeaders);
    await _client.publish(subject, encoded.bytes, headers: wireHeaders);
    messagingConfig.recordMetric(
      'podbus.messages.published',
      attributes: {'transport': 'nats', 'subject': subject},
    );
    messagingConfig.recordDuration(
      'podbus.publish.duration',
      messagingConfig.now().difference(startedAt),
      attributes: {'transport': 'nats', 'subject': subject},
    );
  }

  @override
  Future<Subscription> subscribe<T>(
    String subject, {
    String? queueGroup,
    int concurrency = 1,
    required MessageHandler<T> handler,
  }) async {
    _ensureAvailable();
    if (concurrency < 1) {
      throw const MessagingConfigurationException(
        'Subscription concurrency must be greater than zero.',
      );
    }

    final natsSubscription = _client.subscribe(subject, queueGroup: queueGroup);
    late final _NatsSubscription subscription;
    subscription = _NatsSubscription(
      natsSubscription,
      concurrency: concurrency,
      messagingConfig: messagingConfig,
      onClose: () => _subscriptions.remove(subscription),
    );
    _subscriptions.add(subscription);

    subscription.listen((message) async {
      final decoded = await _decode<T>(message);
      final context = _NatsMessageContext(
        message: message,
        messagingConfig: messagingConfig,
      );
      await handler(context, decoded);
    });

    return subscription;
  }

  @override
  Future<TResponse> request<TRequest, TResponse>(
    String subject,
    TRequest payload, {
    MessageHeaders? headers,
    Duration? timeout,
  }) async {
    _ensureAvailable();
    final encoded = await _codec.encode(payload);
    final wireHeaders = _headersFor(headers ?? MessageHeaders(), encoded);
    messagingConfig.validateRawOutbound(encoded.bytes, wireHeaders);
    final response = await _client.request(
      subject,
      encoded.bytes,
      timeout: timeout ?? config.requestTimeout,
      headers: wireHeaders,
    );
    return _decode<TResponse>(response);
  }

  @override
  Future<HealthCheckResult> healthCheck() async {
    if (!_client.isConnected) {
      return HealthCheckResult.unhealthy(
        message: _closing
            ? 'NATS client is draining.'
            : 'NATS client is not connected.',
        details: {'subscriptions': _subscriptions.length},
      );
    }

    final subscriptionErrors = [
      for (final subscription in _subscriptions)
        if (subscription.lastError != null) subscription.lastError.toString(),
    ];

    try {
      await _client.flush();
      if (subscriptionErrors.isNotEmpty) {
        return HealthCheckResult.degraded(
          message: 'NATS is connected, but a subscription reported an error.',
          details: {
            'subscriptions': _subscriptions.length,
            'subscriptionErrors': subscriptionErrors,
          },
        );
      }
      return HealthCheckResult.healthy(
        message: 'NATS client is connected.',
        details: {'subscriptions': _subscriptions.length},
      );
    } on Object catch (error, stackTrace) {
      return HealthCheckResult.unhealthy(
        message: 'NATS health check failed.',
        details: {
          'error': messagingConfig.limits.truncateError(error),
          'stackTrace': messagingConfig.limits.truncateError(stackTrace),
        },
      );
    }
  }

  Future<T> _decode<T>(NatsClientMessage message) {
    messagingConfig.limits.validatePayload(message.bytes);
    messagingConfig.limits.validateHeaders(message.headers);
    return _codec.decode<T>(
      EncodedMessage(
        bytes: message.bytes,
        contentType:
            message.headers[PodBusWireHeaders.contentType] ??
            JsonMessageCodec.contentType,
        schemaVersion:
            int.tryParse(
              message.headers[PodBusWireHeaders.schemaVersion] ?? '',
            ) ??
            1,
        messageType: message.headers[PodBusWireHeaders.messageType],
      ),
    );
  }

  Map<String, String> _headersFor(
    MessageHeaders headers,
    EncodedMessage encoded,
  ) {
    return {
      for (final MapEntry(:key, :value) in headers.toMap().entries)
        if (value != null) key: value.toString(),
      PodBusWireHeaders.contentType: encoded.contentType,
      PodBusWireHeaders.schemaVersion: encoded.schemaVersion.toString(),
      if (encoded.messageType != null)
        PodBusWireHeaders.messageType: encoded.messageType!,
    };
  }

  void _ensureAvailable() {
    if (_closing || !_client.isConnected) {
      throw const MessagingConnectionException(
        'NATS message bus is not connected.',
      );
    }
  }
}

final class _NatsSubscription implements Subscription {
  _NatsSubscription(
    this._delegate, {
    required this.concurrency,
    required this.messagingConfig,
    required this.onClose,
  });

  final NatsClientSubscription _delegate;
  final int concurrency;
  final MessagingConfig messagingConfig;
  final void Function() onClose;
  final Set<Future<void>> _active = {};
  StreamSubscription<NatsClientMessage>? _streamSubscription;
  Object? lastError;
  StackTrace? lastStackTrace;
  var _closed = false;

  void listen(Future<void> Function(NatsClientMessage message) onMessage) {
    _streamSubscription = _delegate.messages.listen(
      (message) {
        if (_closed) {
          return;
        }
        if (_active.length >= concurrency) {
          _streamSubscription?.pause();
        }
        late final Future<void> task;
        task = Future<void>.sync(() => onMessage(message))
            .then<void>(
              (_) {},
              onError: (Object error, StackTrace stackTrace) {
                lastError = error;
                lastStackTrace = stackTrace;
                messagingConfig.log(
                  MessagingLogLevel.error,
                  'NATS subscription handler failed.',
                  error: error,
                  stackTrace: stackTrace,
                  attributes: {'transport': 'nats'},
                );
              },
            )
            .whenComplete(() {
              _active.remove(task);
              if (!_closed && _active.length < concurrency) {
                _streamSubscription?.resume();
              }
            });
        _active.add(task);
      },
      onError: (Object error, StackTrace stackTrace) {
        lastError = error;
        lastStackTrace = stackTrace;
        messagingConfig.log(
          MessagingLogLevel.error,
          'NATS subscription stream failed.',
          error: error,
          stackTrace: stackTrace,
          attributes: {'transport': 'nats'},
        );
      },
    );
  }

  @override
  Future<void> close() async {
    if (_closed) {
      return;
    }
    _closed = true;
    await _streamSubscription?.cancel();
    await _delegate.close();
    if (_active.isNotEmpty) {
      await Future.wait(
        _active.toList(),
      ).timeout(messagingConfig.shutdownTimeout);
    }
    onClose();
  }
}

final class _NatsMessageContext implements MessageContext {
  _NatsMessageContext({
    required NatsClientMessage message,
    required this.messagingConfig,
  }) : subject = message.subject,
       headers = MessageHeaders.fromMap(message.headers),
       rawMessage = message,
       _message = message;

  final NatsClientMessage _message;
  final MessagingConfig messagingConfig;

  MessageCodec get _codec => messagingConfig.codec;

  @override
  final String subject;

  @override
  final MessageHeaders headers;

  @override
  final Object? rawMessage;

  @override
  Future<void> ack() async {}

  @override
  Future<void> extendVisibility(Duration duration) {
    throw const MessagingUnsupportedException(
      'NATS Core subscriptions do not support visibility extension.',
    );
  }

  @override
  Future<void> nak({Duration? delay}) {
    throw const MessagingUnsupportedException(
      'NATS Core subscriptions do not support negative acknowledgements.',
    );
  }

  @override
  Future<void> reply<T>(T payload, {MessageHeaders? headers}) async {
    final encoded = await _codec.encode(payload);
    final wireHeaders = {
      for (final MapEntry(:key, :value)
          in (headers ?? MessageHeaders()).toMap().entries)
        if (value != null) key: value.toString(),
      PodBusWireHeaders.contentType: encoded.contentType,
      PodBusWireHeaders.schemaVersion: encoded.schemaVersion.toString(),
      if (encoded.messageType != null)
        PodBusWireHeaders.messageType: encoded.messageType!,
    };
    messagingConfig.validateRawOutbound(encoded.bytes, wireHeaders);
    final sent = await _message.respond(encoded.bytes, headers: wireHeaders);
    if (!sent) {
      throw const MessagingUnsupportedException(
        'NATS message does not have a reply subject.',
      );
    }
  }

  @override
  Future<void> terminate() {
    throw const MessagingUnsupportedException(
      'NATS Core subscriptions do not support termination.',
    );
  }
}
