import 'dart:math';

import 'package:podbus_core/podbus_core.dart';

final class W3cTraceContext {
  const W3cTraceContext({
    required this.traceId,
    required this.spanId,
    this.sampled = true,
    this.traceState,
  });

  static const traceParentHeader = 'traceparent';
  static const traceStateHeader = 'tracestate';

  final String traceId;
  final String spanId;
  final bool sampled;
  final String? traceState;

  String get traceParent =>
      '00-$traceId-$spanId-${sampled ? '01' : '00'}';

  factory W3cTraceContext.root({
    bool sampled = true,
    Random? random,
  }) {
    final secure = random ?? Random.secure();
    return W3cTraceContext(
      traceId: _randomHex(16, secure),
      spanId: _randomHex(8, secure),
      sampled: sampled,
    );
  }

  W3cTraceContext child({Random? random}) {
    return W3cTraceContext(
      traceId: traceId,
      spanId: _randomHex(8, random ?? Random.secure()),
      sampled: sampled,
      traceState: traceState,
    );
  }

  MessageHeaders inject(MessageHeaders headers) {
    return headers.copyWith(
      traceId: traceId,
      custom: {
        ...headers.custom,
        traceParentHeader: traceParent,
        if (traceState != null) traceStateHeader: traceState!,
      },
    );
  }

  static W3cTraceContext? extract(MessageHeaders headers) {
    final value = headers.custom[traceParentHeader];
    if (value == null) {
      return null;
    }
    return tryParse(value, traceState: headers.custom[traceStateHeader]);
  }

  static W3cTraceContext? tryParse(String value, {String? traceState}) {
    final parts = value.toLowerCase().split('-');
    if (parts.length != 4 || parts[0] != '00') {
      return null;
    }
    final traceId = parts[1];
    final spanId = parts[2];
    final flags = parts[3];
    if (!_validHex(traceId, 32) ||
        !_validHex(spanId, 16) ||
        !_validHex(flags, 2) ||
        _allZero(traceId) ||
        _allZero(spanId)) {
      return null;
    }
    return W3cTraceContext(
      traceId: traceId,
      spanId: spanId,
      sampled: (int.parse(flags, radix: 16) & 1) == 1,
      traceState: traceState,
    );
  }

  static bool _validHex(String value, int length) {
    return value.length == length && RegExp(r'^[0-9a-f]+$').hasMatch(value);
  }

  static bool _allZero(String value) => RegExp(r'^0+$').hasMatch(value);

  static String _randomHex(int bytes, Random random) {
    final buffer = StringBuffer();
    for (var index = 0; index < bytes; index += 1) {
      buffer.write(random.nextInt(256).toRadixString(16).padLeft(2, '0'));
    }
    return buffer.toString();
  }
}
