import 'exceptions.dart';

final class MessageHeaders {
  MessageHeaders({
    this.correlationId,
    this.causationId,
    this.tenantId,
    this.userId,
    this.traceId,
    this.idempotencyKey,
    this.attempt = 1,
    Map<String, String> custom = const {},
  }) : custom = Map.unmodifiable(custom) {
    if (attempt < 1) {
      throw const MessagingConfigurationException(
        'Message header attempt must be greater than zero.',
      );
    }

    final reserved = custom.keys.where(_reservedHeaderNames.contains).toList();
    if (reserved.isNotEmpty) {
      throw MessagingConfigurationException(
        'Custom headers cannot use reserved names: ${reserved.join(', ')}.',
      );
    }
  }

  factory MessageHeaders.fromMap(Map<String, Object?> map) {
    final custom = <String, String>{};

    for (final MapEntry(:key, :value) in map.entries) {
      if (!_reservedHeaderNames.contains(key) && value != null) {
        custom[key] = value.toString();
      }
    }

    return MessageHeaders(
      correlationId: map[_correlationId] as String?,
      causationId: map[_causationId] as String?,
      tenantId: map[_tenantId] as String?,
      userId: map[_userId] as String?,
      traceId: map[_traceId] as String?,
      idempotencyKey: map[_idempotencyKey] as String?,
      attempt: switch (map[_attempt]) {
        final int value => value,
        final String value => int.parse(value),
        null => 1,
        final Object value => throw MessagingConfigurationException(
          'Message header attempt must be an int or string, got '
          '${value.runtimeType}.',
        ),
      },
      custom: custom,
    );
  }

  static const _correlationId = 'correlationId';
  static const _causationId = 'causationId';
  static const _tenantId = 'tenantId';
  static const _userId = 'userId';
  static const _traceId = 'traceId';
  static const _idempotencyKey = 'idempotencyKey';
  static const _attempt = 'attempt';

  static const _reservedHeaderNames = {
    _correlationId,
    _causationId,
    _tenantId,
    _userId,
    _traceId,
    _idempotencyKey,
    _attempt,
  };

  final String? correlationId;
  final String? causationId;
  final String? tenantId;
  final String? userId;
  final String? traceId;
  final String? idempotencyKey;
  final int attempt;
  final Map<String, String> custom;

  MessageHeaders copyWith({
    String? correlationId,
    String? causationId,
    String? tenantId,
    String? userId,
    String? traceId,
    String? idempotencyKey,
    int? attempt,
    Map<String, String>? custom,
  }) {
    return MessageHeaders(
      correlationId: correlationId ?? this.correlationId,
      causationId: causationId ?? this.causationId,
      tenantId: tenantId ?? this.tenantId,
      userId: userId ?? this.userId,
      traceId: traceId ?? this.traceId,
      idempotencyKey: idempotencyKey ?? this.idempotencyKey,
      attempt: attempt ?? this.attempt,
      custom: custom ?? this.custom,
    );
  }

  MessageHeaders incrementAttempt() => copyWith(attempt: attempt + 1);

  MessageHeaders withoutIdempotencyKey() {
    return MessageHeaders(
      correlationId: correlationId,
      causationId: causationId,
      tenantId: tenantId,
      userId: userId,
      traceId: traceId,
      attempt: attempt,
      custom: custom,
    );
  }

  Map<String, Object?> toMap() {
    return {
      if (correlationId != null) _correlationId: correlationId,
      if (causationId != null) _causationId: causationId,
      if (tenantId != null) _tenantId: tenantId,
      if (userId != null) _userId: userId,
      if (traceId != null) _traceId: traceId,
      if (idempotencyKey != null) _idempotencyKey: idempotencyKey,
      _attempt: attempt,
      ...custom,
    };
  }
}
