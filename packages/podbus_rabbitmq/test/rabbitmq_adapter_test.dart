import 'dart:async';
import 'dart:typed_data';

import 'package:dart_amqp/dart_amqp.dart' as amqp;
import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_rabbitmq/podbus_rabbitmq.dart';
import 'package:podbus_rabbitmq/src/rabbitmq_adapter.dart';
import 'package:test/test.dart';

void main() {
  group('DartRabbitMqAdapter', () {
    test(
      'waits for publisher confirmation before completing publish',
      () async {
        final client = FakeAmqpClient();
        final adapter = DartRabbitMqAdapter(client: client);
        await adapter.connect(_config());
        await adapter.declareExchange(name: 'podbus.events', durable: true);

        var completed = false;
        final publishFuture = adapter
            .publish(
              exchange: 'podbus.events',
              routingKey: 'leads.created',
              bytes: [1, 2, 3],
              headers: const {'podbus-content-type': 'application/json'},
              persistent: true,
            )
            .then((_) => completed = true);

        await Future<void>.delayed(Duration.zero);
        expect(client.channelRef.confirmRequested, isTrue);
        expect(completed, isFalse);

        final properties = client.channelRef.exchangeRef.lastProperties;
        expect(properties, isNotNull);
        client.channelRef.confirmPublish(properties!, published: true);

        await publishFuture.timeout(_testTimeout);
        expect(completed, isTrue);
      },
    );

    test('fails publish when publisher confirmation is nacked', () async {
      final client = FakeAmqpClient();
      final adapter = DartRabbitMqAdapter(client: client);
      await adapter.connect(_config());
      await adapter.declareExchange(name: 'podbus.events', durable: true);

      final publishFuture = adapter.publish(
        exchange: 'podbus.events',
        routingKey: 'leads.created',
        bytes: [1, 2, 3],
        headers: const {'podbus-content-type': 'application/json'},
        persistent: true,
      );

      await Future<void>.delayed(Duration.zero);
      final properties = client.channelRef.exchangeRef.lastProperties;
      expect(properties, isNotNull);
      client.channelRef.confirmPublish(properties!, published: false);

      await expectLater(
        publishFuture,
        throwsA(isA<MessagingConnectionException>()),
      );
    });

    test('times out when publisher confirmation does not arrive', () async {
      final client = FakeAmqpClient();
      final adapter = DartRabbitMqAdapter(client: client);
      await adapter.connect(
        _config(publisherConfirmTimeout: const Duration(milliseconds: 20)),
      );
      await adapter.declareExchange(name: 'podbus.events', durable: true);

      await expectLater(
        adapter.publish(
          exchange: 'podbus.events',
          routingKey: 'leads.created',
          bytes: [1, 2, 3],
          headers: const {'podbus-content-type': 'application/json'},
          persistent: true,
        ),
        throwsA(isA<MessagingTimeoutException>()),
      );
    });
  });
}

const _testTimeout = Duration(seconds: 2);

RabbitMqMessagingConfig _config({Duration? publisherConfirmTimeout}) {
  return RabbitMqMessagingConfig(
    uri: Uri.parse('amqp://guest:guest@localhost:5672'),
    exchange: 'podbus',
    deadLetterExchange: 'podbus.dead',
    publisherConfirmTimeout:
        publisherConfirmTimeout ?? const Duration(seconds: 1),
  );
}

final class FakeAmqpClient implements amqp.Client {
  final channelRef = FakeAmqpChannel();
  final _errors = StreamController<Exception>.broadcast();
  var connected = false;
  var closed = false;

  @override
  bool get handshaking => false;

  @override
  amqp.ConnectionSettings get settings => amqp.ConnectionSettings();

  @override
  amqp.TuningSettings get tuningSettings => amqp.TuningSettings();

  @override
  Future<void> connect() async {
    connected = true;
  }

  @override
  Future<void> close() async {
    closed = true;
    await _errors.close();
  }

  @override
  Future<amqp.Channel> channel() async => channelRef;

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
}

final class FakeAmqpChannel implements amqp.Channel {
  final exchangeRef = FakeAmqpExchange();
  final _publishNotifications =
      StreamController<amqp.PublishNotification>.broadcast();
  var confirmRequested = false;
  var closed = false;

  void confirmPublish(
    amqp.MessageProperties properties, {
    required bool published,
  }) {
    _publishNotifications.add(
      FakePublishNotification(properties: properties, published: published),
    );
  }

  @override
  Future<amqp.Channel> close() async {
    closed = true;
    await _publishNotifications.close();
    return this;
  }

  @override
  Future<void> confirmPublishedMessages() async {
    confirmRequested = true;
  }

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
    exchangeRef
      ..exchangeName = name
      ..exchangeType = type;
    return exchangeRef;
  }

  @override
  StreamSubscription<amqp.PublishNotification> publishNotifier(
    void Function(amqp.PublishNotification notification) onData, {
    Function? onError,
    void Function()? onDone,
    bool? cancelOnError,
  }) {
    return _publishNotifications.stream.listen(
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

final class FakeAmqpExchange implements amqp.Exchange {
  String exchangeName = '';
  amqp.ExchangeType exchangeType = amqp.ExchangeType.TOPIC;
  Object? lastMessage;
  String? lastRoutingKey;
  amqp.MessageProperties? lastProperties;

  @override
  amqp.Channel get channel => throw UnimplementedError();

  @override
  String get name => exchangeName;

  @override
  amqp.ExchangeType get type => exchangeType;

  @override
  void publish(
    Object message,
    String? routingKey, {
    amqp.MessageProperties? properties,
    bool mandatory = false,
    bool immediate = false,
  }) {
    expect(message, isA<Uint8List>());
    lastMessage = message;
    lastRoutingKey = routingKey;
    lastProperties = properties;
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

final class FakePublishNotification implements amqp.PublishNotification {
  const FakePublishNotification({
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
