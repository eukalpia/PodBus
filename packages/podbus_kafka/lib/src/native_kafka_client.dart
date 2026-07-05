// ignore_for_file: implementation_imports

import 'dart:convert';
import 'dart:ffi' as ffi;
import 'dart:io';

import 'package:ffi/ffi.dart';
import 'package:kafka_dart/src/infrastructure/bindings/rdkafka_bindings.g.dart';
import 'package:podbus_core/podbus_core.dart';

final class NativeKafkaLibrary {
  const NativeKafkaLibrary({this.libraryPaths = const []});

  static const pathEnvironmentKey = 'PODBUS_LIBRDKAFKA_PATH';

  final List<String> libraryPaths;

  RdkafkaBindings open() {
    final failures = <String>[];
    for (final path in _candidatePaths()) {
      try {
        return RdkafkaBindings(ffi.DynamicLibrary.open(path));
      } on Object catch (error) {
        failures.add('$path: $error');
      }
    }

    throw MessagingConfigurationException(
      'Unable to load librdkafka. Install librdkafka or set '
      '$pathEnvironmentKey to the native library path.',
      cause: failures.join('\n'),
    );
  }

  Iterable<String> _candidatePaths() sync* {
    final explicitPath = Platform.environment[pathEnvironmentKey];
    if (explicitPath != null && explicitPath.trim().isNotEmpty) {
      yield explicitPath;
    }

    yield* libraryPaths;

    if (Platform.isMacOS) {
      yield 'librdkafka.dylib';
      yield '/opt/homebrew/lib/librdkafka.dylib';
      yield '/opt/homebrew/opt/librdkafka/lib/librdkafka.dylib';
      yield '/usr/local/lib/librdkafka.dylib';
      return;
    }

    if (Platform.isLinux) {
      yield 'librdkafka.so.1';
      yield 'librdkafka.so';
      yield '/usr/lib/librdkafka.so.1';
      yield '/usr/local/lib/librdkafka.so.1';
      return;
    }

    if (Platform.isWindows) {
      yield 'rdkafka.dll';
      return;
    }

    throw MessagingConfigurationException(
      'Platform ${Platform.operatingSystem} is not supported by the native '
      'Kafka adapter.',
    );
  }
}

final class NativeKafkaProducer {
  NativeKafkaProducer._(this._bindings, this._kafka);

  final RdkafkaBindings _bindings;
  ffi.Pointer<rd_kafka_t>? _kafka;

  static NativeKafkaProducer connect(
    RdkafkaBindings bindings,
    Map<String, String> properties,
  ) {
    final kafka = _createClient(
      bindings,
      rd_kafka_type_t.RD_KAFKA_PRODUCER,
      properties,
    );
    return NativeKafkaProducer._(bindings, kafka);
  }

  void produce({
    required String topic,
    required List<int> payload,
    String? key,
  }) {
    final kafka = _requireKafka();
    final topicName = topic.toNativeUtf8();
    final payloadPtr = calloc<ffi.Uint8>(payload.length);
    ffi.Pointer<Utf8>? keyPtr;

    for (var i = 0; i < payload.length; i += 1) {
      payloadPtr[i] = payload[i];
    }

    final topicHandle = _bindings.rd_kafka_topic_new(
      kafka,
      topicName.cast(),
      ffi.nullptr,
    );
    if (topicHandle == ffi.nullptr) {
      calloc.free(topicName);
      calloc.free(payloadPtr);
      throw MessagingConnectionException(
        'Unable to create Kafka topic handle for $topic.',
      );
    }

    try {
      if (key != null) {
        keyPtr = key.toNativeUtf8();
      }
      final result = _bindings.rd_kafka_produce(
        topicHandle,
        -1,
        RD_KAFKA_MSG_F_COPY,
        payloadPtr.cast(),
        payload.length,
        keyPtr?.cast() ?? ffi.nullptr,
        key == null ? 0 : utf8.encode(key).length,
        ffi.nullptr,
      );
      if (result == -1) {
        throw MessagingConnectionException(
          'Failed to produce Kafka message to $topic.',
        );
      }
      _bindings.rd_kafka_poll(kafka, 0);
    } finally {
      _bindings.rd_kafka_topic_destroy(topicHandle);
      calloc.free(topicName);
      calloc.free(payloadPtr);
      if (keyPtr != null) {
        calloc.free(keyPtr);
      }
    }
  }

  void flush(Duration timeout) {
    final result = _bindings.rd_kafka_flush(
      _requireKafka(),
      timeout.inMilliseconds,
    );
    _ensureNoError(_bindings, result, 'Kafka producer flush failed.');
  }

  void close({Duration timeout = const Duration(seconds: 10)}) {
    final kafka = _kafka;
    if (kafka == null) {
      return;
    }
    _bindings.rd_kafka_flush(kafka, timeout.inMilliseconds);
    _bindings.rd_kafka_destroy(kafka);
    _kafka = null;
  }

  ffi.Pointer<rd_kafka_t> _requireKafka() {
    final kafka = _kafka;
    if (kafka == null) {
      throw const MessagingConnectionException(
        'Kafka producer is not connected.',
      );
    }
    return kafka;
  }
}

final class NativeKafkaConsumer {
  NativeKafkaConsumer._(this._bindings, this._kafka);

  final RdkafkaBindings _bindings;
  ffi.Pointer<rd_kafka_t>? _kafka;
  ffi.Pointer<rd_kafka_message_t>? _pendingMessage;

  static NativeKafkaConsumer connect({
    required RdkafkaBindings bindings,
    required List<String> topics,
    required Map<String, String> properties,
  }) {
    final kafka = _createClient(
      bindings,
      rd_kafka_type_t.RD_KAFKA_CONSUMER,
      properties,
    );
    _ensureNoError(
      bindings,
      bindings.rd_kafka_poll_set_consumer(kafka),
      'Kafka consumer poll setup failed.',
    );
    final consumer = NativeKafkaConsumer._(bindings, kafka);
    consumer.subscribe(topics);
    return consumer;
  }

  void subscribe(List<String> topics) {
    if (topics.isEmpty) {
      throw const MessagingConfigurationException(
        'Kafka consumer requires at least one topic.',
      );
    }

    final topicList = _bindings.rd_kafka_topic_partition_list_new(
      topics.length,
    );
    if (topicList == ffi.nullptr) {
      throw const MessagingConnectionException(
        'Unable to allocate Kafka topic list.',
      );
    }

    try {
      for (final topic in topics) {
        final topicName = topic.toNativeUtf8();
        try {
          _bindings.rd_kafka_topic_partition_list_add(
            topicList,
            topicName.cast(),
            -1,
          );
        } finally {
          calloc.free(topicName);
        }
      }

      final result = _bindings.rd_kafka_subscribe(_requireKafka(), topicList);
      _ensureNoError(_bindings, result, 'Kafka consumer subscribe failed.');
    } finally {
      _bindings.rd_kafka_topic_partition_list_destroy(topicList);
    }
  }

  NativeKafkaRecord? poll(Duration timeout) {
    _releasePendingMessage();

    final message = _bindings.rd_kafka_consumer_poll(
      _requireKafka(),
      timeout.inMilliseconds,
    );
    if (message == ffi.nullptr) {
      return null;
    }

    final value = message.ref;
    if (value.err != rd_kafka_resp_err_t.RD_KAFKA_RESP_ERR_NO_ERROR) {
      final description = _messageError(message);
      _bindings.rd_kafka_message_destroy(message);
      throw MessagingConnectionException(
        'Kafka consumer poll failed: $description.',
      );
    }

    _pendingMessage = message;
    final topicName = _bindings.rd_kafka_topic_name(value.rkt);
    final keyBytes = _copyBytes(value.key, value.key_len);
    return NativeKafkaRecord(
      topic: topicName.cast<Utf8>().toDartString(),
      payload: _copyBytes(value.payload, value.len),
      key: keyBytes.isEmpty ? null : utf8.decode(keyBytes),
      partition: value.partition,
      offset: value.offset,
    );
  }

  void commit() {
    final kafka = _requireKafka();
    final message = _pendingMessage;
    if (message == null) {
      return;
    }

    final result = _bindings.rd_kafka_commit_message(kafka, message, 0);
    _ensureNoError(_bindings, result, 'Kafka offset commit failed.');
    _releasePendingMessage();
  }

  void close() {
    final kafka = _kafka;
    if (kafka == null) {
      return;
    }

    _releasePendingMessage();
    _bindings.rd_kafka_unsubscribe(kafka);
    _bindings.rd_kafka_consumer_close(kafka);
    _bindings.rd_kafka_destroy(kafka);
    _kafka = null;
  }

  ffi.Pointer<rd_kafka_t> _requireKafka() {
    final kafka = _kafka;
    if (kafka == null) {
      throw const MessagingConnectionException(
        'Kafka consumer is not connected.',
      );
    }
    return kafka;
  }

  void _releasePendingMessage() {
    final message = _pendingMessage;
    if (message == null) {
      return;
    }
    _bindings.rd_kafka_message_destroy(message);
    _pendingMessage = null;
  }

  String _messageError(ffi.Pointer<rd_kafka_message_t> message) {
    final error = _bindings.rd_kafka_message_errstr(message);
    if (error == ffi.nullptr) {
      return _errorDescription(_bindings, message.ref.err);
    }
    return error.cast<Utf8>().toDartString();
  }
}

final class NativeKafkaRecord {
  const NativeKafkaRecord({
    required this.topic,
    required this.payload,
    required this.key,
    required this.partition,
    required this.offset,
  });

  final String topic;
  final List<int> payload;
  final String? key;
  final int partition;
  final int offset;
}

ffi.Pointer<rd_kafka_t> _createClient(
  RdkafkaBindings bindings,
  rd_kafka_type_t type,
  Map<String, String> properties,
) {
  final config = bindings.rd_kafka_conf_new();
  if (config == ffi.nullptr) {
    throw const MessagingConnectionException(
      'Unable to allocate Kafka client configuration.',
    );
  }

  var configOwned = true;
  try {
    for (final MapEntry(:key, :value) in properties.entries) {
      _setConfig(bindings, config, key, value);
    }

    final errorBuffer = calloc<ffi.Char>(512);
    try {
      final kafka = bindings.rd_kafka_new(type, config, errorBuffer, 512);
      if (kafka == ffi.nullptr) {
        throw MessagingConnectionException(
          'Unable to create Kafka client: '
          '${errorBuffer.cast<Utf8>().toDartString()}.',
        );
      }
      configOwned = false;
      return kafka;
    } finally {
      calloc.free(errorBuffer);
    }
  } finally {
    if (configOwned) {
      bindings.rd_kafka_conf_destroy(config);
    }
  }
}

void _setConfig(
  RdkafkaBindings bindings,
  ffi.Pointer<rd_kafka_conf_t> config,
  String key,
  String value,
) {
  final nativeKey = key.toNativeUtf8();
  final nativeValue = value.toNativeUtf8();
  final errorBuffer = calloc<ffi.Char>(256);

  try {
    final result = bindings.rd_kafka_conf_set(
      config,
      nativeKey.cast(),
      nativeValue.cast(),
      errorBuffer,
      256,
    );
    if (result != rd_kafka_conf_res_t.RD_KAFKA_CONF_OK) {
      throw MessagingConfigurationException(
        'Invalid Kafka configuration "$key": '
        '${errorBuffer.cast<Utf8>().toDartString()}.',
      );
    }
  } finally {
    calloc.free(nativeKey);
    calloc.free(nativeValue);
    calloc.free(errorBuffer);
  }
}

List<int> _copyBytes(ffi.Pointer<ffi.Void> pointer, int length) {
  if (pointer == ffi.nullptr || length == 0) {
    return const [];
  }

  return pointer.cast<ffi.Uint8>().asTypedList(length).toList();
}

void _ensureNoError(
  RdkafkaBindings bindings,
  rd_kafka_resp_err_t result,
  String message,
) {
  if (result == rd_kafka_resp_err_t.RD_KAFKA_RESP_ERR_NO_ERROR) {
    return;
  }

  throw MessagingConnectionException(
    '$message ${_errorDescription(bindings, result)}.',
  );
}

String _errorDescription(RdkafkaBindings bindings, rd_kafka_resp_err_t error) {
  return bindings.rd_kafka_err2str(error).cast<Utf8>().toDartString();
}
