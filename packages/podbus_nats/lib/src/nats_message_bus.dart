import 'dart:async';

import 'package:podbus_core/podbus_core.dart';

import 'config.dart';
import 'nats_client_adapter.dart';

final class NatsMessageBus implements MessageBus {
  NatsMessageBus({
    required this.config,
    NatsClientAdapter? clientAdapter,
    MessageCodec? codec,
  }) : _client = clientAdapter ?? DartNatsClientAdapter(),
       _codec = codec ?? const JsonMessageCodec();

  static const _contentTypeHeader = 'podbus-content-type';
  static const _schemaVersionHeader = 'podbus-schema-version';

  final NatsMessagingConfig config;
  final NatsClientAdapter _client;
  final MessageCodec _codec;
  final List<_NatsSubscription> _subscriptions = [];

  @override
  Future<void> connect() async {
    await _client.connect(config);
  }

  @override
  Future<void> close({Duration? timeout}) async {
    await _client.drain();
    for (final subscription in _subscriptions.toList()) {
      await subscription.close();
    }
    _subscriptions.clear();
    await _client.close();
  }

  @override
  Future<void> publish<T>(
    String subject,
    T payload, {
    MessageHeaders? headers,
  }) async {
    final encoded = await _codec.encode(payload);
    await _client.publish(
      subject,
      encoded.bytes,
      headers: _headersFor(headers ?? MessageHeaders(), encoded),
    );
  }

  @override
  Future<Subscription> subscribe<T>(
    String subject, {
    String? queueGroup,
    required MessageHandler<T> handler,
  }) async {
    final natsSubscription = _client.subscribe(subject, queueGroup: queueGroup);
    late final _NatsSubscription subscription;
    subscription = _NatsSubscription(
      natsSubscription,
      onClose: () => _subscriptions.remove(subscription),
    );
    _subscriptions.add(subscription);

    subscription.listen((message) async {
      final decoded = await _decode<T>(message);
      final context = _NatsMessageContext(
        message: message,
        codec: _codec,
        contentTypeHeader: _contentTypeHeader,
        schemaVersionHeader: _schemaVersionHeader,
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
    final encoded = await _codec.encode(payload);
    final response = await _client.request(
      subject,
      encoded.bytes,
      timeout: timeout ?? config.requestTimeout,
      headers: _headersFor(headers ?? MessageHeaders(), encoded),
    );
    return _decode<TResponse>(response);
  }

  @override
  Future<HealthCheckResult> healthCheck() async {
    if (!_client.isConnected) {
      return HealthCheckResult.unhealthy(
        message: 'NATS client is not connected.',
      );
    }

    try {
      await _client.flush();
      return HealthCheckResult.healthy(message: 'NATS client is connected.');
    } on Object catch (error, stackTrace) {
      return HealthCheckResult(
        status: HealthStatus.unhealthy,
        checkedAt: DateTime.now(),
        message: 'NATS health check failed.',
        details: {
          'error': error.toString(),
          'stackTrace': stackTrace.toString(),
        },
      );
    }
  }

  Future<T> _decode<T>(NatsClientMessage message) {
    return _codec.decode<T>(
      EncodedMessage(
        bytes: message.bytes,
        contentType:
            message.headers[_contentTypeHeader] ?? JsonMessageCodec.contentType,
        schemaVersion:
            int.tryParse(message.headers[_schemaVersionHeader] ?? '') ?? 1,
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
      _contentTypeHeader: encoded.contentType,
      _schemaVersionHeader: encoded.schemaVersion.toString(),
    };
  }
}

final class _NatsSubscription implements Subscription {
  _NatsSubscription(this._delegate, {required this.onClose});

  final NatsClientSubscription _delegate;
  final void Function() onClose;
  StreamSubscription<NatsClientMessage>? _streamSubscription;
  var _closed = false;

  void listen(Future<void> Function(NatsClientMessage message) onMessage) {
    _streamSubscription = _delegate.messages.listen((message) {
      unawaited(onMessage(message));
    });
  }

  @override
  Future<void> close() async {
    if (_closed) {
      return;
    }
    _closed = true;
    await _streamSubscription?.cancel();
    await _delegate.close();
    onClose();
  }
}

final class _NatsMessageContext implements MessageContext {
  _NatsMessageContext({
    required NatsClientMessage message,
    required this._codec,
    required this._contentTypeHeader,
    required this._schemaVersionHeader,
  }) : subject = message.subject,
       headers = MessageHeaders.fromMap(message.headers),
       rawMessage = message,
       _message = message;

  final NatsClientMessage _message;
  final MessageCodec _codec;
  final String _contentTypeHeader;
  final String _schemaVersionHeader;

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
    final sent = await _message.respond(
      encoded.bytes,
      headers: {
        for (final MapEntry(:key, :value)
            in (headers ?? MessageHeaders()).toMap().entries)
          if (value != null) key: value.toString(),
        _contentTypeHeader: encoded.contentType,
        _schemaVersionHeader: encoded.schemaVersion.toString(),
      },
    );
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
