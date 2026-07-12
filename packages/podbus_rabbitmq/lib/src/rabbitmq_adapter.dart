// ignore_for_file: prefer_initializing_formals

import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:dart_amqp/dart_amqp.dart' as amqp;
import 'package:podbus_core/podbus_core.dart';

import 'config.dart';

abstract interface class RabbitMqAdapter {
  bool get isConnected;

  Future<void> connect(RabbitMqMessagingConfig config);

  Future<void> close();

  Future<void> declareExchange({required String name, required bool durable});

  Future<void> declareQueue({
    required String name,
    required bool durable,
    bool exclusive = false,
    bool autoDelete = false,
    Map<String, Object?> arguments,
  });

  Future<void> bindQueue({
    required String queue,
    required String exchange,
    required String routingKey,
  });

  Future<void> setPrefetchCount(int count);

  Future<void> publish({
    required String exchange,
    required String routingKey,
    required List<int> bytes,
    required Map<String, String> headers,
    required bool persistent,
  });

  Future<RabbitMqConsumer> consume({
    required String queue,
    required bool noAck,
  });
}

abstract interface class RabbitMqConsumer {
  Stream<RabbitMqDelivery> get deliveries;

  Future<void> close();
}

abstract interface class RabbitMqDelivery {
  String get routingKey;

  List<int> get bytes;

  Map<String, String> get headers;

  Future<void> ack();

  Future<void> nack({required bool requeue});
}

final class DartRabbitMqAdapter implements RabbitMqAdapter {
  DartRabbitMqAdapter({amqp.Client? client}) : _client = client;

  amqp.Client? _client;
  amqp.Channel? _publisherChannel;
  amqp.Channel? _consumerChannel;
  StreamSubscription<amqp.PublishNotification>? _publishNotifications;
  StreamSubscription<amqp.BasicReturnMessage>? _returnedMessages;
  StreamSubscription<Exception>? _clientErrors;

  final Map<String, amqp.Exchange> _publisherExchanges = {};
  final Map<String, amqp.Exchange> _consumerExchanges = {};
  final Map<String, amqp.Queue> _queues = {};
  final Map<String, Completer<void>> _pendingPublishes = {};

  var _publisherConfirmTimeout = const Duration(seconds: 5);
  var _mandatoryPublish = true;
  var _publishSequence = 0;
  var _publishEpoch = 0;
  var _connected = false;
  Future<void> _publishTail = Future<void>.value();

  @override
  bool get isConnected => _connected;

  @override
  Future<void> connect(RabbitMqMessagingConfig config) async {
    final uri = config.uri;
    final settings = amqp.ConnectionSettings(
      host: uri.host,
      port: uri.hasPort ? uri.port : (uri.scheme == 'amqps' ? 5671 : 5672),
      virtualHost: _virtualHost(uri),
      authProvider: amqp.PlainAuthenticator(
        Uri.decodeComponent(_username(uri.userInfo)),
        Uri.decodeComponent(_password(uri.userInfo) ?? 'guest'),
      ),
      connectTimeout: config.connectTimeout,
      connectionName: config.connectionName,
      maxConnectionAttempts: config.maxConnectionAttempts,
      reconnectWaitTime: config.reconnectWaitTime,
      tlsContext: uri.scheme == 'amqps'
          ? (config.tlsContext ?? SecurityContext.defaultContext)
          : null,
      onBadCertificate: config.onBadCertificate,
    );

    final client = _client ?? amqp.Client(settings: settings);
    await client.connect();
    _client = client;

    await _clientErrors?.cancel();
    _clientErrors = client.errorListener(
      _handleClientError,
      onError: (Object error, StackTrace stackTrace) {
        _handleClientError(
          MessagingConnectionException(
            'RabbitMQ client error stream failed.',
            cause: error,
            stackTrace: stackTrace,
          ),
        );
      },
      onDone: () {
        if (_connected) {
          _handleClientError(
            const MessagingConnectionException(
              'RabbitMQ client error stream closed unexpectedly.',
            ),
          );
        }
      },
    );

    final publisherChannel = await client.channel();
    final consumerChannel = await client.channel();
    _publisherChannel = publisherChannel;
    _consumerChannel = consumerChannel;
    _publisherConfirmTimeout = config.publisherConfirmTimeout;
    _mandatoryPublish = config.mandatoryPublish;

    await publisherChannel.confirmPublishedMessages();
    _publishNotifications = publisherChannel.publishNotifier(
      _handlePublishNotification,
      onError: (Object error, StackTrace stackTrace) {
        _failPendingPublishes(
          MessagingConnectionException(
            'RabbitMQ publisher confirmation stream failed.',
            cause: error,
            stackTrace: stackTrace,
          ),
        );
      },
    );
    _returnedMessages = publisherChannel.basicReturnListener(
      _handleReturnedMessage,
      onError: (Object error, StackTrace stackTrace) {
        _failPendingPublishes(
          MessagingConnectionException(
            'RabbitMQ returned-message stream failed.',
            cause: error,
            stackTrace: stackTrace,
          ),
        );
      },
    );

    _publishEpoch += 1;
    _publishTail = Future<void>.value();
    _connected = true;
  }

  @override
  Future<void> close() async {
    _connected = false;
    _publishEpoch += 1;
    _publishTail = Future<void>.value();
    _failPendingPublishes(
      const MessagingConnectionException(
        'RabbitMQ adapter closed before publisher confirmation.',
      ),
    );

    await _publishNotifications?.cancel();
    await _returnedMessages?.cancel();
    await _clientErrors?.cancel();
    _publishNotifications = null;
    _returnedMessages = null;
    _clientErrors = null;

    await _publisherChannel?.close();
    await _consumerChannel?.close();
    await _client?.close();
    _publisherChannel = null;
    _consumerChannel = null;
    _client = null;
    _publisherExchanges.clear();
    _consumerExchanges.clear();
    _queues.clear();
  }

  @override
  Future<void> declareExchange({required String name, required bool durable}) {
    return _channelOperation('exchange declaration', () async {
      _publisherExchanges[name] = await _publisherCh.exchange(
        name,
        amqp.ExchangeType.TOPIC,
        durable: durable,
      );
      _consumerExchanges[name] = await _consumerCh.exchange(
        name,
        amqp.ExchangeType.TOPIC,
        durable: durable,
      );
    });
  }

  @override
  Future<void> declareQueue({
    required String name,
    required bool durable,
    bool exclusive = false,
    bool autoDelete = false,
    Map<String, Object?> arguments = const {},
  }) {
    return _channelOperation('queue declaration', () async {
      final queueArguments = <String, Object>{};
      for (final MapEntry(:key, :value) in arguments.entries) {
        if (value != null) {
          queueArguments[key] = value;
        }
      }
      _queues[name] = await _consumerCh.queue(
        name,
        durable: durable,
        exclusive: exclusive,
        autoDelete: autoDelete,
        arguments: queueArguments,
      );
    });
  }

  @override
  Future<void> bindQueue({
    required String queue,
    required String exchange,
    required String routingKey,
  }) {
    return _channelOperation('queue binding', () async {
      final queueRef =
          _queues[queue] ?? await _consumerCh.queue(queue, declare: false);
      final exchangeRef =
          _consumerExchanges[exchange] ??
          await _consumerCh.exchange(
            exchange,
            amqp.ExchangeType.TOPIC,
            declare: false,
          );
      await queueRef.bind(exchangeRef, routingKey);
      _queues[queue] = queueRef;
      _consumerExchanges[exchange] = exchangeRef;
    });
  }

  @override
  Future<RabbitMqConsumer> consume({
    required String queue,
    required bool noAck,
  }) {
    return _channelOperation('consumer creation', () async {
      final queueRef =
          _queues[queue] ?? await _consumerCh.queue(queue, declare: false);
      _queues[queue] = queueRef;
      final consumer = await queueRef.consume(noAck: noAck);
      return _DartRabbitMqConsumer(consumer);
    });
  }

  @override
  Future<void> setPrefetchCount(int count) {
    return _channelOperation(
      'QoS configuration',
      () => _consumerCh.qos(null, count, global: false),
    );
  }

  @override
  Future<void> publish({
    required String exchange,
    required String routingKey,
    required List<int> bytes,
    required Map<String, String> headers,
    required bool persistent,
  }) {
    final epoch = _publishEpoch;

    // dart_amqp 0.3.1 mutates its live pending-confirm key iterable when
    // RabbitMQ emits a multi-ack. Keeping one outstanding confirm per channel
    // avoids that unsafe branch while preserving the broker-confirm contract.
    final operation = _publishTail.then<void>((_) async {
      if (!_connected || epoch != _publishEpoch) {
        throw const MessagingConnectionException(
          'RabbitMQ publisher connection changed before publish started.',
        );
      }
      await _publishConfirmed(
        exchange: exchange,
        routingKey: routingKey,
        bytes: bytes,
        headers: headers,
        persistent: persistent,
      );
    });

    // A failed publish must not poison the serialization lane for later calls.
    _publishTail = operation.then<void>(
      (_) {},
      onError: (Object _, StackTrace __) {},
    );
    return operation;
  }

  Future<void> _publishConfirmed({
    required String exchange,
    required String routingKey,
    required List<int> bytes,
    required Map<String, String> headers,
    required bool persistent,
  }) async {
    final exchangeRef =
        _publisherExchanges[exchange] ??
        await _publisherCh.exchange(
          exchange,
          amqp.ExchangeType.TOPIC,
          declare: false,
        );
    final publishId = _nextPublishId();
    final published = Completer<void>();
    _pendingPublishes[publishId] = published;
    final properties = amqp.MessageProperties()
      ..headers = headers
      ..corellationId = publishId
      ..persistent = persistent
      ..contentType = headers['podbus-content-type'];

    try {
      exchangeRef.publish(
        Uint8List.fromList(bytes),
        routingKey,
        properties: properties,
        mandatory: _mandatoryPublish,
      );
      _publisherExchanges[exchange] = exchangeRef;
      await published.future.timeout(
        _publisherConfirmTimeout,
        onTimeout: () {
          _pendingPublishes.remove(publishId);
          throw MessagingTimeoutException(
            'Timed out waiting for RabbitMQ publisher confirmation.',
          );
        },
      );
    } on MessagingException {
      rethrow;
    } on Object catch (error, stackTrace) {
      _pendingPublishes.remove(publishId);
      final failure = MessagingConnectionException(
        'Failed to publish RabbitMQ message.',
        cause: error,
        stackTrace: stackTrace,
      );
      _handleClientError(failure);
      throw failure;
    }
  }

  amqp.Channel get _publisherCh {
    final channel = _publisherChannel;
    if (channel == null) {
      throw StateError('RabbitMQ publisher channel is not connected.');
    }
    return channel;
  }

  amqp.Channel get _consumerCh {
    final channel = _consumerChannel;
    if (channel == null) {
      throw StateError('RabbitMQ consumer channel is not connected.');
    }
    return channel;
  }

  Future<T> _channelOperation<T>(
    String operation,
    Future<T> Function() action,
  ) async {
    try {
      return await action();
    } on MessagingException {
      rethrow;
    } on Object catch (error, stackTrace) {
      final failure = MessagingConnectionException(
        'RabbitMQ $operation failed.',
        cause: error,
        stackTrace: stackTrace,
      );
      _handleClientError(failure);
      throw failure;
    }
  }

  void _handleClientError(Exception error) {
    _connected = false;
    _publishEpoch += 1;
    _failPendingPublishes(
      MessagingConnectionException(
        'RabbitMQ client or channel failed.',
        cause: error,
      ),
    );
  }

  String _nextPublishId() {
    _publishSequence += 1;
    return 'podbus-${DateTime.now().microsecondsSinceEpoch}-$_publishSequence';
  }

  void _handlePublishNotification(amqp.PublishNotification notification) {
    final publishId = notification.properties?.corellationId;
    if (publishId == null) {
      return;
    }
    final published = _pendingPublishes.remove(publishId);
    if (published == null || published.isCompleted) {
      return;
    }
    if (notification.published) {
      published.complete();
      return;
    }
    published.completeError(
      MessagingConnectionException(
        'RabbitMQ publisher confirmation was nacked by the broker.',
        cause: notification.message,
      ),
    );
  }

  void _handleReturnedMessage(amqp.BasicReturnMessage message) {
    final publishId = message.properties?.corellationId;
    if (publishId == null) {
      return;
    }
    final published = _pendingPublishes.remove(publishId);
    if (published == null || published.isCompleted) {
      return;
    }
    published.completeError(
      MessagingConnectionException(
        'RabbitMQ returned unroutable message '
        '(${message.replyCode} ${message.replyText}) for '
        '${message.exchangeName}:${message.routingKey}.',
      ),
    );
  }

  void _failPendingPublishes(MessagingException error) {
    final pending = List<Completer<void>>.of(_pendingPublishes.values);
    _pendingPublishes.clear();
    for (final published in pending) {
      if (!published.isCompleted) {
        published.completeError(error);
      }
    }
  }
}

final class _DartRabbitMqConsumer implements RabbitMqConsumer {
  _DartRabbitMqConsumer(this._consumer) {
    _subscription = _consumer.listen(
      (message) {
        _controller.add(_DartRabbitMqDelivery(message));
      },
      onError: _controller.addError,
      onDone: _controller.close,
    );
  }

  final amqp.Consumer _consumer;
  final _controller = StreamController<RabbitMqDelivery>.broadcast();
  late final StreamSubscription<amqp.AmqpMessage> _subscription;

  @override
  Stream<RabbitMqDelivery> get deliveries => _controller.stream;

  @override
  Future<void> close() async {
    await _subscription.cancel();
    await _consumer.cancel();
    await _controller.close();
  }
}

final class _DartRabbitMqDelivery implements RabbitMqDelivery {
  const _DartRabbitMqDelivery(this._message);

  final amqp.AmqpMessage _message;

  @override
  List<int> get bytes => _message.payload ?? Uint8List(0);

  @override
  Map<String, String> get headers {
    final headers = _message.properties?.headers ?? const <String, Object?>{};
    return {
      for (final MapEntry(:key, :value) in headers.entries)
        if (value != null) key: value.toString(),
    };
  }

  @override
  String get routingKey => _message.routingKey ?? '';

  @override
  Future<void> ack() async {
    _message.ack();
  }

  @override
  Future<void> nack({required bool requeue}) async {
    _message.reject(requeue);
  }
}

String _virtualHost(Uri uri) {
  if (uri.pathSegments.isEmpty || uri.pathSegments.single.isEmpty) {
    return '/';
  }
  return Uri.decodeComponent(uri.pathSegments.join('/'));
}

String _username(String userInfo) {
  if (userInfo.isEmpty) {
    return 'guest';
  }
  final separator = userInfo.indexOf(':');
  if (separator < 0) {
    return userInfo;
  }
  return userInfo.substring(0, separator);
}

String? _password(String userInfo) {
  final separator = userInfo.indexOf(':');
  if (separator < 0) {
    return null;
  }
  return userInfo.substring(separator + 1);
}
