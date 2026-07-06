// ignore_for_file: prefer_initializing_formals

import 'dart:async';
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
  amqp.Channel? _channel;
  StreamSubscription<amqp.PublishNotification>? _publishNotifications;
  final Map<String, amqp.Exchange> _exchanges = {};
  final Map<String, amqp.Queue> _queues = {};
  final Map<String, Completer<void>> _pendingPublishes = {};
  var _publisherConfirmTimeout = const Duration(seconds: 5);
  var _publishSequence = 0;
  var _connected = false;

  @override
  bool get isConnected => _connected;

  @override
  Future<void> bindQueue({
    required String queue,
    required String exchange,
    required String routingKey,
  }) async {
    final queueRef = _queues[queue] ?? await _ch.queue(queue, declare: false);
    final exchangeRef =
        _exchanges[exchange] ??
        await _ch.exchange(exchange, amqp.ExchangeType.TOPIC, declare: false);
    await queueRef.bind(exchangeRef, routingKey);
    _queues[queue] = queueRef;
    _exchanges[exchange] = exchangeRef;
  }

  @override
  Future<void> close() async {
    _connected = false;
    _failPendingPublishes(
      const MessagingConnectionException(
        'RabbitMQ adapter closed before publisher confirmation.',
      ),
    );
    await _publishNotifications?.cancel();
    _publishNotifications = null;
    await _channel?.close();
    await _client?.close();
    _channel = null;
    _client = null;
    _exchanges.clear();
    _queues.clear();
  }

  @override
  Future<void> connect(RabbitMqMessagingConfig config) async {
    final uri = config.uri;
    final settings = amqp.ConnectionSettings(
      host: uri.host,
      port: uri.hasPort ? uri.port : 5672,
      virtualHost: _virtualHost(uri),
      authProvider: amqp.PlainAuthenticator(
        Uri.decodeComponent(_username(uri.userInfo)),
        Uri.decodeComponent(_password(uri.userInfo) ?? 'guest'),
      ),
      connectTimeout: config.connectTimeout,
      connectionName: 'podbus-rabbitmq',
    );
    final client = _client ?? amqp.Client(settings: settings);
    await client.connect();
    _client = client;
    _channel = await client.channel();
    _publisherConfirmTimeout = config.publisherConfirmTimeout;
    await _channel!.confirmPublishedMessages();
    _publishNotifications = _channel!.publishNotifier(
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
    _connected = true;
  }

  @override
  Future<RabbitMqConsumer> consume({
    required String queue,
    required bool noAck,
  }) async {
    final queueRef = _queues[queue] ?? await _ch.queue(queue, declare: false);
    _queues[queue] = queueRef;
    final consumer = await queueRef.consume(noAck: noAck);
    return _DartRabbitMqConsumer(consumer);
  }

  @override
  Future<void> declareExchange({
    required String name,
    required bool durable,
  }) async {
    _exchanges[name] = await _ch.exchange(
      name,
      amqp.ExchangeType.TOPIC,
      durable: durable,
    );
  }

  @override
  Future<void> declareQueue({
    required String name,
    required bool durable,
    Map<String, Object?> arguments = const {},
  }) async {
    final queueArguments = <String, Object>{};
    for (final MapEntry(:key, :value) in arguments.entries) {
      if (value != null) {
        queueArguments[key] = value;
      }
    }

    _queues[name] = await _ch.queue(
      name,
      durable: durable,
      arguments: queueArguments,
    );
  }

  @override
  Future<void> publish({
    required String exchange,
    required String routingKey,
    required List<int> bytes,
    required Map<String, String> headers,
    required bool persistent,
  }) async {
    final exchangeRef =
        _exchanges[exchange] ??
        await _ch.exchange(exchange, amqp.ExchangeType.TOPIC, declare: false);
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
      );
      _exchanges[exchange] = exchangeRef;
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
      throw MessagingConnectionException(
        'Failed to publish RabbitMQ message.',
        cause: error,
        stackTrace: stackTrace,
      );
    }
  }

  @override
  Future<void> setPrefetchCount(int count) async {
    await _ch.qos(null, count, global: false);
  }

  amqp.Channel get _ch {
    final channel = _channel;
    if (channel == null) {
      throw StateError('RabbitMQ adapter is not connected.');
    }
    return channel;
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
