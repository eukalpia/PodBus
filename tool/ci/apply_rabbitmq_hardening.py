from pathlib import Path


def replace(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text()
    if old not in text:
        raise SystemExit(f"Expected fragment not found in {path}: {old[:80]!r}")
    file.write_text(text.replace(old, new))


adapter = "packages/podbus_rabbitmq/lib/src/rabbitmq_adapter.dart"
replace(
    adapter,
    """  Future<void> declareQueue({
    required String name,
    required bool durable,
    Map<String, Object?> arguments,
  });
""",
    """  Future<void> declareQueue({
    required String name,
    required bool durable,
    bool exclusive = false,
    bool autoDelete = false,
    Map<String, Object?> arguments,
  });
""",
)
replace(
    adapter,
    """  amqp.Client? _client;
  amqp.Channel? _channel;
  StreamSubscription<amqp.PublishNotification>? _publishNotifications;
  final Map<String, amqp.Exchange> _exchanges = {};
  final Map<String, amqp.Queue> _queues = {};
""",
    """  amqp.Client? _client;
  amqp.Channel? _publisherChannel;
  amqp.Channel? _consumerChannel;
  StreamSubscription<amqp.PublishNotification>? _publishNotifications;
  StreamSubscription<amqp.BasicReturnMessage>? _returnedMessages;
  final Map<String, amqp.Exchange> _publisherExchanges = {};
  final Map<String, amqp.Exchange> _consumerExchanges = {};
  final Map<String, amqp.Queue> _queues = {};
""",
)
replace(
    adapter,
    """  var _publisherConfirmTimeout = const Duration(seconds: 5);
  var _publishSequence = 0;
""",
    """  var _publisherConfirmTimeout = const Duration(seconds: 5);
  var _mandatoryPublish = true;
  var _publishSequence = 0;
""",
)
replace(
    adapter,
    """    final queueRef = _queues[queue] ?? await _ch.queue(queue, declare: false);
    final exchangeRef =
        _exchanges[exchange] ??
        await _ch.exchange(exchange, amqp.ExchangeType.TOPIC, declare: false);
    await queueRef.bind(exchangeRef, routingKey);
    _queues[queue] = queueRef;
    _exchanges[exchange] = exchangeRef;
""",
    """    final queueRef =
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
""",
)
replace(
    adapter,
    """    await _publishNotifications?.cancel();
    _publishNotifications = null;
    await _channel?.close();
    await _client?.close();
    _channel = null;
    _client = null;
    _exchanges.clear();
    _queues.clear();
""",
    """    await _publishNotifications?.cancel();
    await _returnedMessages?.cancel();
    _publishNotifications = null;
    _returnedMessages = null;
    await _publisherChannel?.close();
    await _consumerChannel?.close();
    await _client?.close();
    _publisherChannel = null;
    _consumerChannel = null;
    _client = null;
    _publisherExchanges.clear();
    _consumerExchanges.clear();
    _queues.clear();
""",
)
replace(
    adapter,
    """      port: uri.hasPort ? uri.port : 5672,
""",
    """      port: uri.hasPort ? uri.port : (uri.scheme == 'amqps' ? 5671 : 5672),
""",
)
replace(
    adapter,
    """      connectTimeout: config.connectTimeout,
      connectionName: 'podbus-rabbitmq',
    );
""",
    """      connectTimeout: config.connectTimeout,
      connectionName: 'podbus-rabbitmq',
      maxConnectionAttempts: config.maxConnectionAttempts,
      reconnectWaitTime: config.reconnectWaitTime,
      tlsContext: uri.scheme == 'amqps'
          ? (config.tlsContext ?? SecurityContext.defaultContext)
          : null,
      onBadCertificate: config.onBadCertificate,
    );
""",
)
replace(
    adapter,
    """    _client = client;
    _channel = await client.channel();
    _publisherConfirmTimeout = config.publisherConfirmTimeout;
    await _channel!.confirmPublishedMessages();
    _publishNotifications = _channel!.publishNotifier(
""",
    """    _client = client;
    _publisherChannel = await client.channel();
    _consumerChannel = await client.channel();
    _publisherConfirmTimeout = config.publisherConfirmTimeout;
    _mandatoryPublish = config.mandatoryPublish;
    await _publisherChannel!.confirmPublishedMessages();
    _publishNotifications = _publisherChannel!.publishNotifier(
""",
)
replace(
    adapter,
    """    );
    _connected = true;
  }

  @override
  Future<RabbitMqConsumer> consume({
""",
    """    );
    _returnedMessages = _publisherChannel!.basicReturnListener(
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
    _connected = true;
  }

  @override
  Future<RabbitMqConsumer> consume({
""",
)
replace(
    adapter,
    """    final queueRef = _queues[queue] ?? await _ch.queue(queue, declare: false);
""",
    """    final queueRef =
        _queues[queue] ?? await _consumerCh.queue(queue, declare: false);
""",
)
replace(
    adapter,
    """    _exchanges[name] = await _ch.exchange(
      name,
      amqp.ExchangeType.TOPIC,
      durable: durable,
    );
""",
    """    _publisherExchanges[name] = await _publisherCh.exchange(
      name,
      amqp.ExchangeType.TOPIC,
      durable: durable,
    );
    _consumerExchanges[name] = await _consumerCh.exchange(
      name,
      amqp.ExchangeType.TOPIC,
      durable: durable,
    );
""",
)
replace(
    adapter,
    """  Future<void> declareQueue({
    required String name,
    required bool durable,
    Map<String, Object?> arguments = const {},
  }) async {
""",
    """  Future<void> declareQueue({
    required String name,
    required bool durable,
    bool exclusive = false,
    bool autoDelete = false,
    Map<String, Object?> arguments = const {},
  }) async {
""",
)
replace(
    adapter,
    """    _queues[name] = await _ch.queue(
      name,
      durable: durable,
      arguments: queueArguments,
    );
""",
    """    _queues[name] = await _consumerCh.queue(
      name,
      durable: durable,
      exclusive: exclusive,
      autoDelete: autoDelete,
      arguments: queueArguments,
    );
""",
)
replace(
    adapter,
    """    final exchangeRef =
        _exchanges[exchange] ??
        await _ch.exchange(exchange, amqp.ExchangeType.TOPIC, declare: false);
""",
    """    final exchangeRef =
        _publisherExchanges[exchange] ??
        await _publisherCh.exchange(
          exchange,
          amqp.ExchangeType.TOPIC,
          declare: false,
        );
""",
)
replace(
    adapter,
    """        properties: properties,
      );
      _exchanges[exchange] = exchangeRef;
""",
    """        properties: properties,
        mandatory: _mandatoryPublish,
      );
      _publisherExchanges[exchange] = exchangeRef;
""",
)
replace(
    adapter,
    """    await _ch.qos(null, count, global: false);
  }

  amqp.Channel get _ch {
    final channel = _channel;
    if (channel == null) {
      throw StateError('RabbitMQ adapter is not connected.');
    }
    return channel;
  }
""",
    """    await _consumerCh.qos(null, count, global: false);
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
""",
)
replace(
    adapter,
    """  void _failPendingPublishes(MessagingException error) {
""",
    """  void _handleReturnedMessage(amqp.BasicReturnMessage message) {
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
""",
)

bus = "packages/podbus_rabbitmq/lib/src/rabbitmq_message_bus.dart"
replace(
    bus,
    """      await _adapter.declareExchange(
        name: config.deadLetterExchange,
        durable: config.durable,
      );
      _connected = true;
""",
    """      await _adapter.declareExchange(
        name: config.deadLetterExchange,
        durable: config.durable,
      );
      if (config.useBrokerRetryQueues) {
        await _adapter.declareExchange(
          name: config.effectiveRetryExchange,
          durable: config.durable,
        );
      }
      _connected = true;
""",
)
replace(
    bus,
    """    await _declareBoundQueue(queue: queue, routingKey: subject);
""",
    """    await _declareBoundQueue(
      queue: queue,
      routingKey: subject,
      ephemeral: queueGroup == null,
    );
""",
)
replace(
    bus,
    """      final delay = retryPolicy.delayForAttempt(attempt);
      if (delay > Duration.zero) {
        await Future<void>.delayed(delay);
      }
      await _publishRaw(
        exchange: config.exchange,
        routingKey: worker.topic,
        bytes: delivery.bytes,
        headers: _rawHeadersWithAttempt(delivery.headers, attempt + 1),
      );
""",
    """      final delay = retryPolicy.delayForAttempt(attempt);
      if (config.useBrokerRetryQueues && delay > Duration.zero) {
        await _publishRetry(
          worker: worker,
          delivery: delivery,
          attempt: attempt + 1,
          delay: delay,
        );
      } else {
        if (delay > Duration.zero) {
          await Future<void>.delayed(delay);
        }
        await _publishRaw(
          exchange: config.exchange,
          routingKey: worker.topic,
          bytes: delivery.bytes,
          headers: _rawHeadersWithAttempt(delivery.headers, attempt + 1),
        );
      }
""",
)
replace(
    bus,
    """  Future<void> _publishDeadLetter(
""",
    """  Future<void> _publishRetry({
    required _RabbitMqWorker<Object?> worker,
    required RabbitMqDelivery delivery,
    required int attempt,
    required Duration delay,
  }) async {
    final delayMs = delay.inMilliseconds.clamp(1, 2147483647);
    final routingKey = '${worker.topic}.retry.$delayMs';
    final queue = _namedQueue('retry', worker.topic, '${delayMs}ms');
    await _adapter.declareQueue(
      name: queue,
      durable: config.durable,
      arguments: {
        'x-message-ttl': delayMs,
        'x-dead-letter-exchange': config.exchange,
        'x-dead-letter-routing-key': worker.topic,
      },
    );
    await _adapter.bindQueue(
      queue: queue,
      exchange: config.effectiveRetryExchange,
      routingKey: routingKey,
    );
    await _publishRaw(
      exchange: config.effectiveRetryExchange,
      routingKey: routingKey,
      bytes: delivery.bytes,
      headers: _rawHeadersWithAttempt(delivery.headers, attempt),
    );
    messagingConfig.recordMetric(
      'podbus.jobs.retried',
      attributes: {
        'transport': 'rabbitmq',
        'topic': worker.topic,
        'delayMs': delayMs,
      },
    );
  }

  Future<void> _publishDeadLetter(
""",
)
replace(
    bus,
    """  Future<void> _declareBoundQueue({
    required String queue,
    required String routingKey,
  }) async {
    await _adapter.declareQueue(
      name: queue,
      durable: config.durable,
      arguments: {'x-dead-letter-exchange': config.deadLetterExchange},
    );
""",
    """  Future<void> _declareBoundQueue({
    required String queue,
    required String routingKey,
    bool ephemeral = false,
  }) async {
    await _adapter.declareQueue(
      name: queue,
      durable: ephemeral ? false : config.durable,
      exclusive: ephemeral,
      autoDelete: ephemeral,
      arguments: {'x-dead-letter-exchange': config.deadLetterExchange},
    );
""",
)

test = "packages/podbus_rabbitmq/test/rabbitmq_message_bus_test.dart"
replace(
    test,
    """      expect(adapter.declaredExchanges, [
        ('podbus.events', true),
        ('podbus.dead', true),
      ]);
""",
    """      expect(adapter.declaredExchanges, [
        ('podbus.events', true),
        ('podbus.dead', true),
        ('podbus.events.retry', true),
      ]);
""",
)
replace(
    test,
    """  Future<void> declareQueue({
    required String name,
    required bool durable,
    Map<String, Object?> arguments = const {},
  }) async {
    declaredQueues.add(FakeQueueDeclaration(name, durable, arguments));
  }
""",
    """  Future<void> declareQueue({
    required String name,
    required bool durable,
    bool exclusive = false,
    bool autoDelete = false,
    Map<String, Object?> arguments = const {},
  }) async {
    declaredQueues.add(
      FakeQueueDeclaration(
        name,
        durable,
        exclusive,
        autoDelete,
        arguments,
      ),
    );
  }
""",
)
replace(
    test,
    """final class FakeQueueDeclaration {
  const FakeQueueDeclaration(this.name, this.durable, this.arguments);

  final String name;
  final bool durable;
  final Map<String, Object?> arguments;
}
""",
    """final class FakeQueueDeclaration {
  const FakeQueueDeclaration(
    this.name,
    this.durable,
    this.exclusive,
    this.autoDelete,
    this.arguments,
  );

  final String name;
  final bool durable;
  final bool exclusive;
  final bool autoDelete;
  final Map<String, Object?> arguments;
}
""",
)

print("RabbitMQ hardening patch applied.")
