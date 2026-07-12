import 'dart:async';
import 'dart:typed_data';

import 'package:dart_amqp/dart_amqp.dart' as amqp;
import 'package:podbus_rabbitmq/podbus_rabbitmq.dart';
import 'package:podbus_rabbitmq/src/rabbitmq_adapter.dart';
import 'package:test/test.dart';

void main() {
  group('RabbitMQ publisher lanes', () {
    test('allows one confirmed publish per channel in parallel', () async {
      final client = _FakeClient();
      final adapter = DartRabbitMqAdapter(client: client);
      await adapter.connect(_config(publisherChannelCount: 2));
      await adapter.declareExchange(name: 'events', durable: true);

      final publishes = [
        _publish(adapter, 1),
        _publish(adapter, 2),
        _publish(adapter, 3),
      ];

      await _waitUntil(
        () => client.channels[0].exchange.publishes.length == 1 &&
            client.channels[1].exchange.publishes.length == 1,
      );
      expect(client.channels[0].exchange.publishes.length, 1);
      expect(client.channels[1].exchange.publishes.length, 1);

      client.channels[0].confirmNext();
      client.channels[1].confirmNext();

      await _waitUntil(
        () => client.channels[0].exchange.publishes.length == 2,
      );
      client.channels[0].confirmNext();

      await Future.wait(publishes).timeout(const Duration(seconds: 2));
      expect(client.channels, hasLength(3));
      expect(
        client.channels
            .take(2)
            .expand((channel) => channel.exchange.publishes)
            .length,
        3,
      );
      await adapter.close();
    });

    test('never has two unconfirmed publishes on one channel', () async {
      final client = _FakeClient();
      final adapter = DartRabbitMqAdapter(client: client);
      await adapter.connect(_config(publisherChannelCount: 1));
      await adapter.declareExchange(name: 'events', durable: true);

      final first = _publish(adapter, 1);
      final second = _publish(adapter, 2);

      await _waitUntil(
        () => client.channels.first.exchange.publishes.length == 1,
      );
      await Future<void>.delayed(const Duration(milliseconds: 10));
      expect(client.channels.first.exchange.publishes.length, 1);

      client.channels.first.confirmNext();
      await _waitUntil(
        () => client.channels.first.exchange.publishes.length == 2,
      );
      client.channels.first.confirmNext();

      await Future.wait([first, second]).timeout(const Duration(seconds: 2));
      await adapter.close();
    });

    test('failed publish does not poison its lane', () async {
      final client = _FakeClient();
      final adapter = DartRabbitMqAdapter(client: client);
      await adapter.connect(_config(publisherChannelCount: 1));
      await adapter.declareExchange(name: 'events', durable: true);

      final first = _publish(adapter, 1);
      await _waitUntil(
        () => client.channels.first.exchange.publishes.length == 1,
      );
      client.channels.first.confirmNext(published: false);
      await expectLater(first, throwsA(isA<Exception>()));

      final second = _publish(adapter, 2);
      await _waitUntil(
        () => client.channels.first.exchange.publishes.length == 2,
      );
      client.channels.first.confirmNext();
      await second.timeout(const Duration(seconds: 2));
      await adapter.close();
    });
  });
}

Future<void> _publish(DartRabbitMqAdapter adapter, int value) {
  return adapter.publish(
    exchange: 'events',
    routingKey: 'events.created',
    bytes: [value],
    headers: const {'podbus-content-type': 'application/octet-stream'},
    persistent: true,
  );
}

RabbitMqMessagingConfig _config({required int publisherChannelCount}) {
  return RabbitMqMessagingConfig(
    uri: Uri.parse('amqp://guest:guest@localhost:5672'),
    exchange: 'events',
    deadLetterExchange: 'events.dead',
    publisherChannelCount: publisherChannelCount,
    publisherConfirmTimeout: const Duration(seconds: 1),
  );
}

Future<void> _waitUntil(bool Function() condition) async {
  final deadline = DateTime.now().add(const Duration(seconds: 2));
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) {
      throw TimeoutException('Condition did not become true.');
    }
    await Future<void>.delayed(const Duration(milliseconds: 1));
  }
}

final class _FakeClient implements amqp.Client {
  final channels = <_FakeChannel>[];
  final _errors = StreamController<Exception>.broadcast();

  @override
  Future<amqp.Channel> channel() async {
    final channel = _FakeChannel();
    channels.add(channel);
    return channel;
  }

  @override
  Future<void> close() async {
    await _errors.close();
  }

  @override
  Future<void> connect() async {}

  @override
  StreamSubscription<Exception> errorListener(
    void Function(Exception error) onData, {
    Function? onError,
    void Function()? onDone,
    bool? cancelOnError,
  }) {
    return _errors.stream.listen(
      onData,
      onError: onError,
      onDone: onDone,
      cancelOnError: cancelOnError,
    );
  }

  @override
  bool get handshaking => false;

  @override
  amqp.ConnectionSettings get settings => amqp.ConnectionSettings();

  @override
  amqp.TuningSettings get tuningSettings => amqp.TuningSettings();
}

final class _FakeChannel implements amqp.Channel {
  final exchange = _FakeExchange();
  final _confirmations =
      StreamController<amqp.PublishNotification>.broadcast();
  final _returns = StreamController<amqp.BasicReturnMessage>.broadcast();
  var _nextConfirmation = 0;

  void confirmNext({bool published = true}) {
    final properties = exchange.publishes[_nextConfirmation].properties;
    _nextConfirmation += 1;
    _confirmations.add(
      _FakePublishNotification(
        properties: properties,
        published: published,
      ),
    );
  }

  @override
  StreamSubscription<amqp.BasicReturnMessage> basicReturnListener(
    void Function(amqp.BasicReturnMessage message) onData, {
    Function? onError,
    void Function()? onDone,
    bool? cancelOnError,
  }) {
    return _returns.stream.listen(
      onData,
      onError: onError,
      onDone: onDone,
      cancelOnError: cancelOnError,
    );
  }

  @override
  Future<amqp.Channel> close() async {
    await _confirmations.close();
    await _returns.close();
    return this;
  }

  @override
  Future<void> confirmPublishedMessages() async {}

  @override
  Future<amqp.Exchange> exchange(
    String name,
    amqp.ExchangeType type, {
    bool passive = false,
    bool durable = false,
    bool noWait = false,
    bool declare = true,
    Map<String, Object> arguments = const {},
  }) async {
    return exchange;
  }

  @override
  StreamSubscription<amqp.PublishNotification> publishNotifier(
    void Function(amqp.PublishNotification notification) onData, {
    Function? onError,
    void Function()? onDone,
    bool? cancelOnError,
  }) {
    return _confirmations.stream.listen(
      onData,
      onError: onError,
      onDone: onDone,
      cancelOnError: cancelOnError,
    );
  }

  @override
  Future<amqp.Channel> qos(
    int? prefetchSize,
    int? prefetchCount, {
    bool global = true,
  }) async {
    return this;
  }

  @override
  Future<amqp.Queue> queue(
    String name, {
    bool passive = false,
    bool durable = false,
    bool exclusive = false,
    bool autoDelete = false,
    bool noWait = false,
    bool declare = true,
    Map<String, Object> arguments = const {},
  }) {
    throw UnimplementedError();
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

final class _FakeExchange implements amqp.Exchange {
  final publishes = <_Published>[];

  @override
  void publish(
    Object message,
    String? routingKey, {
    amqp.MessageProperties? properties,
    bool mandatory = false,
    bool immediate = false,
  }) {
    publishes.add(
      _Published(
        message: message as Uint8List,
        properties: properties!,
      ),
    );
  }

  @override
  amqp.Channel get channel => throw UnimplementedError();

  @override
  String get name => 'events';

  @override
  amqp.ExchangeType get type => amqp.ExchangeType.TOPIC;

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

final class _Published {
  const _Published({required this.message, required this.properties});

  final Uint8List message;
  final amqp.MessageProperties properties;
}

final class _FakePublishNotification implements amqp.PublishNotification {
  const _FakePublishNotification({
    required this.properties,
    required this.published,
  });

  @override
  final Object? message = null;

  @override
  final amqp.MessageProperties properties;

  @override
  final bool published;
}
