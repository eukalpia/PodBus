import 'dart:convert';
import 'dart:typed_data';

import 'exceptions.dart';

abstract interface class MessageCodec {
  Future<EncodedMessage> encode<T>(
    T payload, {
    Object? Function(T payload)? toJson,
    int schemaVersion,
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
  }) : bytes = Uint8List.fromList(bytes);

  final Uint8List bytes;
  final String contentType;
  final int schemaVersion;
}

final class JsonMessageCodec implements MessageCodec {
  const JsonMessageCodec();

  static const contentType = 'application/json';

  @override
  Future<EncodedMessage> encode<T>(
    T payload, {
    Object? Function(T payload)? toJson,
    int schemaVersion = 1,
  }) async {
    try {
      final json = toJson == null ? payload : toJson(payload);
      return EncodedMessage(
        bytes: utf8.encode(jsonEncode(json)),
        contentType: contentType,
        schemaVersion: schemaVersion,
      );
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
      if (decoded is T) {
        return decoded;
      }

      throw MessageCodecException(
        'Decoded JSON value cannot be assigned to $T.',
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
