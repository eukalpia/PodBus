import 'dart:convert';

import 'package:podbus_core/podbus_core.dart';

typedef JsonLogWriter = void Function(String line);

final class JsonMessagingLogSink {
  JsonMessagingLogSink({
    required this.write,
    this.serviceName = 'podbus',
    this.includeStackTraces = false,
    this.maxValueCharacters = 2048,
    Set<String>? sensitiveKeys,
  }) : sensitiveKeys = Set.unmodifiable(
         (sensitiveKeys ?? _defaultSensitiveKeys).map((key) => key.toLowerCase()),
       ) {
    if (maxValueCharacters < 1) {
      throw const MessagingConfigurationException(
        'JSON log maxValueCharacters must be greater than zero.',
      );
    }
  }

  static const _defaultSensitiveKeys = {
    'authorization',
    'cookie',
    'password',
    'secret',
    'token',
    'api_key',
    'apikey',
    'access_token',
    'refresh_token',
    'email',
    'phone',
    'payload',
  };

  final JsonLogWriter write;
  final String serviceName;
  final bool includeStackTraces;
  final int maxValueCharacters;
  final Set<String> sensitiveKeys;

  MessagingLogHook get hook => record;

  void record(MessagingLogEvent event) {
    final body = <String, Object?>{
      'timestamp': event.timestamp.toUtc().toIso8601String(),
      'level': event.level.name,
      'service': serviceName,
      'message': _truncate(event.message),
      'attributes': _redactMap(event.attributes),
      if (event.error != null) 'error': _truncate(event.error.toString()),
      if (includeStackTraces && event.stackTrace != null)
        'stackTrace': _truncate(event.stackTrace.toString()),
    };
    write(jsonEncode(body));
  }

  Map<String, Object?> _redactMap(Map<String, Object?> input) {
    return {
      for (final entry in input.entries)
        entry.key: _isSensitive(entry.key)
            ? '[REDACTED]'
            : _redactValue(entry.value),
    };
  }

  Object? _redactValue(Object? value) {
    return switch (value) {
      null => null,
      final Map<Object?, Object?> map => {
        for (final entry in map.entries)
          entry.key.toString(): _isSensitive(entry.key.toString())
              ? '[REDACTED]'
              : _redactValue(entry.value),
      },
      final Iterable<Object?> values => [
        for (final value in values.take(100)) _redactValue(value),
      ],
      final num value => value,
      final bool value => value,
      final Object value => _truncate(value.toString()),
    };
  }

  bool _isSensitive(String key) {
    final normalized = key.toLowerCase();
    return sensitiveKeys.any(
      (sensitive) =>
          normalized == sensitive || normalized.contains(sensitive),
    );
  }

  String _truncate(String value) {
    if (value.length <= maxValueCharacters) {
      return value;
    }
    return '${value.substring(0, maxValueCharacters)}…';
  }
}
