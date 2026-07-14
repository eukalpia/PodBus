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
  amqp.Channel? _consumerChannel;
  StreamSubscription<Exception>? _clientErrors;
  final List<_PublisherLane> _publisherLanes = [];
  final Map<String, amqp.Exchange> _consumerExchanges = {};
  final Map<String, amqp.Queue> _queues = {};
  final Map<String, Completer<void>> _pendingPublishes = {};

  var _publisherConfirmTimeout = const Duration(seconds: 5);
  var _closeTimeout = const Duration(seconds: 5);
  var _mandatoryPublish = true;
  var _publishSequence = 0;
  var _publishEpoch = 0;
  var _nextPublisherLane = 0;
  var _connected = false;

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

    final lanes = <_PublisherLane>[];
    try {
      for (var index = 0; index < config.publisherChannelCount; index += 1) {
        final channel = await client.channel();
        await channel.confirmPublishedMessages();
        final lane = _PublisherLane(channel);
        lane.publishNotifications = channel.publishNotifier(
          (notification) => _handlePublishNotification(lane, notification),
          onError: (Object error, StackTrace stackTrace) {
            _handleLaneError(
              lane,
              MessagingConnectionException(
                'RabbitMQ publisher confirmation stream failed.',
                cause: error,
                stackTrace: stackTrace,
              ),
            );
          },
        );
        lane.returnedMessages = channel.basicReturnListener(
          (message) => _handleReturnedMessage(lane, message),
          onError: (Object error, StackTrace stackTrace) {
            _handleLaneError(
              lane,
              MessagingConnectionException(
                'RabbitMQ returned-message stream failed.',
                cause: error,
                stackTrace: stackTrace,
              ),
            );
          },
        );
        lanes.add(lane);
      }

      _consumerChannel = await client.channel();
    } on Object {
      for (final lane in lanes) {
        await lane.close().catchError((_) {});
      }
      await client.close().catchError((_) {});
      _client = null;
      rethrow;
    }

    _publisherLanes
      ..clear()
      ..addAll(lanes);
    _publisherConfirmTimeout = config.publisherConfirmTimeout;
    _closeTimeout = config.connectTimeout;
    _mandatoryPublish = config.mandatoryPublish;
    _publishEpoch += 1;
    _nextPublisherLane = 0;
    _connected = true;
  }

  @override
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

  @override
  Future<void> declareExchange({required String name, required bool durable}) {
    return _channelOperation('exchange declaration', () async {
      for (final lane in _publisherLanes) {
        lane.exchanges[name] = await lane.channel.exchange(
          name,
          amqp.ExchangeType.TOPIC,
          durable: durable,
        );
      }
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
    if (!_connected || _publisherLanes.isEmpty) {
      return Future<void>.error(
        const MessagingConnectionException(
          'RabbitMQ publisher is not connected.',
        ),
      );
    }

    final lane = _publisherLanes[_nextPublisherLane % _publisherLanes.length];
    _nextPublisherLane += 1;
    final epoch = _publishEpoch;

    // dart_amqp 0.3.1 mutates its live pending-confirm key iterable when the
    // broker sends a multi-ack. Each lane therefore keeps one unconfirmed
    // publish in flight, while the lane pool preserves parallel throughput.
    final operation = lane.tail.then<void>((_) async {
      if (!_connected || epoch != _publishEpoch) {
        throw const MessagingConnectionException(
          'RabbitMQ publisher connection changed before publish started.',
        );
      }
      await _publishConfirmed(
        lane: lane,
        exchange: exchange,
        routingKey: routingKey,
        bytes: bytes,
        headers: headers,
        persistent: persistent,
      );
    });

    // A failed publish must not poison the lane for later calls.
    lane.tail = operation.then<void>(
      (_) {},
      onError: (Object _, StackTrace __) {},
    );
    return operation;
  }

  Future<void> _publishConfirmed({
    required _PublisherLane lane,
    required String exchange,
    required String routingKey,
    required List<int> bytes,
    required Map<String, String> headers,
    required bool persistent,
  }) async {
    final exchangeRef =
        lane.exchanges[exchange] ??
        await lane.channel.exchange(
          exchange,
          amqp.ExchangeType.TOPIC,
          declare: false,
        );
    final publishId = _nextPublishId();
    final published = Completer<void>();
    _pendingPublishes[publishId] = published;
    lane.pendingPublishId = publishId;
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
      lane.exchanges[exchange] = exchangeRef;
      await published.future.timeout(
        _publisherConfirmTimeout,
        onTimeout: () {
          _pendingPublishes.remove(publishId);
          lane.pendingPublishId = null;
          throw MessagingTimeoutException(
            'Timed out waiting for RabbitMQ publisher confirmation.',
          );
        },
      );
    } on MessagingException {
      rethrow;
    } on Object catch (error, stackTrace) {
      _pendingPublishes.remove(publishId);
      lane.pendingPublishId = null;
      final failure = MessagingConnectionException(
        'Failed to publish RabbitMQ message.',
        cause: error,
        stackTrace: stackTrace,
      );
      _handleClientError(failure);
      throw failure;
    }
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

  void _handleLaneError(
    _PublisherLane lane,
    MessagingConnectionException error,
  ) {
    final publishId = lane.pendingPublishId;
    if (publishId != null) {
      final pending = _pendingPublishes.remove(publishId);
      lane.pendingPublishId = null;
      if (pending != null && !pending.isCompleted) {
        pending.completeError(error);
      }
    }
    _handleClientError(error);
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

  void _handlePublishNotification(
    _PublisherLane lane,
    amqp.PublishNotification notification,
  ) {
    final publishId = notification.properties?.corellationId;
    if (publishId == null) {
      return;
    }
    final published = _pendingPublishes.remove(publishId);
    if (lane.pendingPublishId == publishId) {
      lane.pendingPublishId = null;
    }
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

  void _handleReturnedMessage(
    _PublisherLane lane,
    amqp.BasicReturnMessage message,
  ) {
    final publishId = message.properties?.corellationId;
    if (publishId == null) {
      return;
    }
    final published = _pendingPublishes.remove(publishId);
    if (lane.pendingPublishId == publishId) {
      lane.pendingPublishId = null;
    }
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
    for (final lane in _publisherLanes) {
      lane.pendingPublishId = null;
    }
    for (final published in pending) {
      if (!published.isCompleted) {
        published.completeError(error);
      }
    }
  }
}

final class _PublisherLane {
  _PublisherLane(this.channel);

  final amqp.Channel channel;
  final Map<String, amqp.Exchange> exchanges = {};
  Future<void> tail = Future<void>.value();
  String? pendingPublishId;
  StreamSubscription<amqp.PublishNotification>? publishNotifications;
  StreamSubscription<amqp.BasicReturnMessage>? returnedMessages;

  Future<void> close() async {
    await publishNotifications?.cancel();
    await returnedMessages?.cancel();
    publishNotifications = null;
    returnedMessages = null;
    await channel.close();
    exchanges.clear();
    pendingPublishId = null;
    tail = Future<void>.value();
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
