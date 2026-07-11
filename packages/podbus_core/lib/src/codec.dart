import 'dart:convert';
import 'dart:typed_data';

import 'exceptions.dart';

abstract interface class MessageCodec {
  Future<EncodedMessage> encode<T>(
    T payload, {
    Object? Function(T payload)? toJson,
    int schemaVersion = 1,
  });

  Future<T> decode<T>(
    EncodedMessage encoded, {
    T Function(Object? json)? fromJson,
  });
}

final class EncodedMessage {
  EncodedMessage({
    required List<int> bytes,
    required this.contentType,
    required this.schemaVersion,
    this.messageType,
  }) : bytes = Uint8List.fromList(bytes);

  final Uint8List bytes;
  final String contentType;
  final int schemaVersion;
  final String? messageType;
}

typedef MessageJsonEncoder<T> = Object? Function(T value);
typedef MessageJsonDecoder<T> = T Function(Object? json, int schemaVersion);

final class MessageCodecRegistry {
  final Map<Type, _MessageRegistration> _byDartType = {};
  final Map<String, _MessageRegistration> _byMessageType = {};

  void register<T>({
    required String messageType,
    required MessageJsonEncoder<T> encode,
    required MessageJsonDecoder<T> decode,
    int schemaVersion = 1,
  }) {
    final normalizedType = messageType.trim();
    if (normalizedType.isEmpty) {
      throw const MessagingConfigurationException(
        'Registered message type cannot be empty.',
      );
    }
    if (schemaVersion < 1) {
      throw const MessagingConfigurationException(
        'Registered schema version must be greater than zero.',
      );
    }
    if (_byDartType.containsKey(T)) {
      throw MessagingConfigurationException(
        'A codec is already registered for Dart type $T.',
      );
    }
    if (_byMessageType.containsKey(normalizedType)) {
      throw MessagingConfigurationException(
        'A codec is already registered for message type $normalizedType.',
      );
    }

    final registration = _MessageRegistration(
      dartType: T,
      messageType: normalizedType,
      schemaVersion: schemaVersion,
      encode: (value) => encode(value as T),
      decode: decode,
    );
    _byDartType[T] = registration;
    _byMessageType[normalizedType] = registration;
  }

  bool containsDartType<T>() => _byDartType.containsKey(T);

  bool containsMessageType(String messageType) {
    return _byMessageType.containsKey(messageType);
  }

  _MessageRegistration? _registrationForValue(Object? value) {
    if (value == null) {
      return null;
    }
    return _byDartType[value.runtimeType];
  }

  _MessageRegistration? _registrationForDecode<T>(String? messageType) {
    if (messageType != null) {
      final byName = _byMessageType[messageType];
      if (byName != null) {
        return byName;
      }
    }
    return _byDartType[T];
  }
}

final class JsonMessageCodec implements MessageCodec {
  const JsonMessageCodec({this.registry});

  static const contentType = 'application/json';

  final MessageCodecRegistry? registry;

  @override
  Future<EncodedMessage> encode<T>(
    T payload, {
    Object? Function(T payload)? toJson,
    int schemaVersion = 1,
  }) async {
    try {
      final registration = toJson == null
          ? registry?._registrationForValue(payload)
          : null;
      final json = switch ((toJson, registration)) {
        (final encoder?, _) => encoder(payload),
        (_, final registered?) => registered.encode(payload),
        _ => payload,
      };
      final effectiveSchemaVersion =
          registration?.schemaVersion ?? schemaVersion;
      return EncodedMessage(
        bytes: utf8.encode(jsonEncode(json)),
        contentType: contentType,
        schemaVersion: effectiveSchemaVersion,
        messageType: registration?.messageType,
      );
    } on MessagingException {
      rethrow;
    } on Object catch (error, stackTrace) {
      throw MessageCodecException(
        'Failed to encode payload as JSON.',
        cause: error,
        stackTrace: stackTrace,
      );
    }
  }

  @override
  Future<T> decode<T>(
    EncodedMessage encoded, {
    T Function(Object? json)? fromJson,
  }) async {
    if (encoded.contentType != contentType) {
      throw MessageCodecException(
        'Unsupported content type ${encoded.contentType}.',
      );
    }

    try {
      final decoded = _normalizeJson(jsonDecode(utf8.decode(encoded.bytes)));
      if (fromJson != null) {
        return fromJson(decoded);
      }

      final registration = registry?._registrationForDecode<T>(
        encoded.messageType,
      );
      if (registration != null) {
        final value = registration.decode(decoded, encoded.schemaVersion);
        if (value is T) {
          return value;
        }
        throw MessageCodecException(
          'Registered decoder for ${registration.messageType} returned '
          '${value.runtimeType}, expected $T.',
        );
      }

      if (decoded is T) {
        return decoded;
      }

      throw MessageCodecException(
        'Decoded JSON value cannot be assigned to $T. Register a typed codec '
        'or provide fromJson.',
      );
    } on MessagingException {
      rethrow;
    } on Object catch (error, stackTrace) {
      throw MessageCodecException(
        'Failed to decode JSON payload.',
        cause: error,
        stackTrace: stackTrace,
      );
    }
  }

  Object? _normalizeJson(Object? value) {
    return switch (value) {
      final Map<Object?, Object?> map => <String, Object?>{
        for (final MapEntry(:key, :value) in map.entries)
          if (key is String) key: _normalizeJson(value),
      },
      final List<Object?> list => [
        for (final item in list) _normalizeJson(item),
      ],
      null || bool() || num() || String() => value,
      _ => throw MessageCodecException(
        'Unsupported JSON value ${value.runtimeType}.',
      ),
    };
  }
}

final class _MessageRegistration {
  const _MessageRegistration({
    required this.dartType,
    required this.messageType,
    required this.schemaVersion,
    required this.encode,
    required this.decode,
  });

  final Type dartType;
  final String messageType;
  final int schemaVersion;
  final Object? Function(Object? value) encode;
  final Object? Function(Object? json, int schemaVersion) decode;
}
