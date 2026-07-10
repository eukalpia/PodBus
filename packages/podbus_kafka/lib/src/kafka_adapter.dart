import 'config.dart';
import 'native_kafka_client.dart';

abstract interface class KafkaAdapter {
  bool get isConnected;

  Future<void> connect(KafkaMessagingConfig config);

  Future<void> close();

  Future<void> flush([Duration? timeout]);

  Future<void> produce({
    required String topic,
    required List<int> bytes,
    String? key,
  });

  Future<KafkaAdapterConsumer> consumerFor({
    required List<String> topics,
    required String groupId,
  });
}

abstract interface class KafkaAdapterConsumer {
  Future<KafkaAdapterRecord?> poll(Duration timeout);

  Future<void> commit();

  Future<void> close();
}

final class KafkaAdapterRecord {
  const KafkaAdapterRecord({
    required this.topic,
    required this.bytes,
    required this.key,
    required this.partition,
    required this.offset,
    this.rawMessage,
  });

  final String topic;
  final List<int> bytes;
  final String? key;
  final int partition;
  final int? offset;
  final Object? rawMessage;
}

final class DartKafkaAdapter implements KafkaAdapter {
  DartKafkaAdapter({NativeKafkaLibrary? nativeLibrary})
    : _nativeLibrary = nativeLibrary ?? NativeKafkaLibrary();

  final NativeKafkaLibrary _nativeLibrary;
  NativeKafkaProducer? _producer;
  final List<KafkaAdapterConsumer> _consumers = [];
  KafkaMessagingConfig? _config;
  var _connected = false;

  @override
  bool get isConnected => _connected;

  @override
  Future<void> close() async {
    _connected = false;
    for (final consumer in _consumers.toList()) {
      await consumer.close();
    }
    _consumers.clear();
    _producer?.close();
    _producer = null;
    _config = null;
  }

  @override
  Future<void> connect(KafkaMessagingConfig config) async {
    final bindings = _nativeLibrary.open();
    final producer = NativeKafkaProducer.connect(
      bindings,
      _producerProperties(config),
    );
    _producer = producer;
    _config = config;
    _connected = true;
  }

  @override
  Future<KafkaAdapterConsumer> consumerFor({
    required List<String> topics,
    required String groupId,
  }) async {
    final config = _config;
    if (config == null) {
      throw StateError('Kafka adapter is not connected.');
    }
    final consumer = NativeKafkaConsumer.connect(
      bindings: _nativeLibrary.open(),
      topics: topics,
      properties: _consumerProperties(config, groupId),
    );
    final adapterConsumer = _DartKafkaAdapterConsumer(consumer);
    _consumers.add(adapterConsumer);
    return adapterConsumer;
  }

  @override
  Future<void> flush([Duration? timeout]) async {
    _producer?.flush(timeout ?? const Duration(seconds: 10));
  }

  @override
  Future<void> produce({
    required String topic,
    required List<int> bytes,
    String? key,
  }) async {
    final producer = _producer;
    if (producer == null) {
      throw StateError('Kafka adapter is not connected.');
    }
    producer.produce(topic: topic, payload: bytes, key: key);
    final config = _config;
    if (config?.flushAfterProduce ?? true) {
      producer.flush(config?.requestTimeout ?? const Duration(seconds: 10));
    }
  }

  Map<String, String> _producerProperties(KafkaMessagingConfig config) {
    return {
      'bootstrap.servers': config.brokers.join(','),
      'client.id': config.clientId,
      'enable.idempotence': 'true',
      'acks': 'all',
      ...config.producerProperties,
    };
  }

  Map<String, String> _consumerProperties(
    KafkaMessagingConfig config,
    String groupId,
  ) {
    return {
      'bootstrap.servers': config.brokers.join(','),
      'client.id': config.clientId,
      'group.id': groupId,
      'enable.auto.commit': 'false',
      'enable.auto.offset.store': 'false',
      'auto.offset.reset': 'earliest',
      ...config.consumerProperties,
    };
  }
}

final class _DartKafkaAdapterConsumer implements KafkaAdapterConsumer {
  const _DartKafkaAdapterConsumer(this._consumer);

  final NativeKafkaConsumer _consumer;

  @override
  Future<void> close() async => _consumer.close();

  @override
  Future<void> commit() async => _consumer.commit();

  @override
  Future<KafkaAdapterRecord?> poll(Duration timeout) async {
    final message = _consumer.poll(timeout);
    if (message == null) {
      return null;
    }
    return KafkaAdapterRecord(
      topic: message.topic,
      bytes: message.payload,
      key: message.key,
      partition: message.partition,
      offset: message.offset,
      rawMessage: message,
    );
  }
}
