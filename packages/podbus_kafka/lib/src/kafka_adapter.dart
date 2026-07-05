import 'dart:convert';

import 'package:kafka_dart/kafka_dart.dart' as kafka;

import 'config.dart';

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
  kafka.KafkaProducerService? _producer;
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
    await _producer?.close();
    _producer = null;
    _config = null;
  }

  @override
  Future<void> connect(KafkaMessagingConfig config) async {
    final producer = await kafka.KafkaFactory.createAndInitializeProducer(
      bootstrapServers: config.brokers.join(','),
      additionalProperties: {'client.id': config.clientId},
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
    final consumer = await kafka.KafkaFactory.createAndInitializeConsumer(
      bootstrapServers: config.brokers.join(','),
      groupId: groupId,
      additionalProperties: {
        'client.id': config.clientId,
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'earliest',
      },
    );
    await consumer.subscribe(topics);
    final adapterConsumer = _DartKafkaAdapterConsumer(consumer);
    _consumers.add(adapterConsumer);
    return adapterConsumer;
  }

  @override
  Future<void> flush([Duration? timeout]) async {
    await _producer?.flush(timeout);
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
    await producer.sendMessage(
      topic: topic,
      payload: utf8.decode(bytes),
      key: key,
    );
  }
}

final class _DartKafkaAdapterConsumer implements KafkaAdapterConsumer {
  const _DartKafkaAdapterConsumer(this._consumer);

  final kafka.KafkaConsumerService _consumer;

  @override
  Future<void> close() => _consumer.close();

  @override
  Future<void> commit() => _consumer.commitAsync();

  @override
  Future<KafkaAdapterRecord?> poll(Duration timeout) async {
    final message = await _consumer.pollMessage(timeout);
    if (message == null) {
      return null;
    }
    return KafkaAdapterRecord(
      topic: message.topic.value,
      bytes: utf8.encode(message.payload.value),
      key: message.key.value,
      partition: message.partition.value,
      offset: message.offset,
      rawMessage: message,
    );
  }
}
