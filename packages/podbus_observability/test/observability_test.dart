import 'dart:async';
import 'dart:convert';

import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_observability/podbus_observability.dart';
import 'package:test/test.dart';

void main() {
  group('W3C trace context', () {
    test('round trips through message headers', () {
      const context = W3cTraceContext(
        traceId: '0123456789abcdef0123456789abcdef',
        spanId: '0123456789abcdef',
        traceState: 'vendor=value',
      );

      final headers = context.inject(MessageHeaders(correlationId: 'corr-1'));
      final extracted = W3cTraceContext.extract(headers);

      expect(extracted?.traceId, context.traceId);
      expect(extracted?.spanId, context.spanId);
      expect(extracted?.traceState, 'vendor=value');
      expect(headers.correlationId, 'corr-1');
    });

    test('rejects invalid zero identifiers', () {
      expect(
        W3cTraceContext.tryParse(
          '00-00000000000000000000000000000000-0123456789abcdef-01',
        ),
        isNull,
      );
    });
  });

  test('Prometheus registry bounds cardinality and exports metrics', () {
    final registry = PrometheusRegistry(
      maxSeries: 1,
      allowedLabelKeys: {'transport'},
    );

    registry.record(
      MessagingMetricEvent(
        name: 'podbus.messages.published',
        value: 1,
        timestamp: DateTime.utc(2026),
        attributes: {'transport': 'nats', 'tenantId': 'private'},
      ),
    );
    registry.record(
      MessagingMetricEvent(
        name: 'podbus.messages.published',
        value: 2,
        timestamp: DateTime.utc(2026),
        attributes: {'transport': 'nats'},
      ),
    );
    registry.record(
      MessagingMetricEvent(
        name: 'podbus.messages.failed',
        value: 1,
        timestamp: DateTime.utc(2026),
        attributes: {'transport': 'nats'},
      ),
    );

    final output = registry.render();
    expect(output, contains('podbus_messages_published_total'));
    expect(output, contains('transport="nats"'));
    expect(output, isNot(contains('tenantId')));
    expect(output, contains('podbus_metrics_dropped_series_total 1'));
  });

  test('JSON log sink redacts secrets and personal data', () {
    final lines = <String>[];
    final sink = JsonMessagingLogSink(write: lines.add);

    sink.record(
      MessagingLogEvent(
        level: MessagingLogLevel.error,
        message: 'delivery failed',
        timestamp: DateTime.utc(2026),
        error: StateError('token=secret'),
        attributes: {
          'transport': 'rabbitmq',
          'authorization': 'Bearer secret',
          'profile': {'email': 'person@example.com'},
        },
      ),
    );

    final decoded = jsonDecode(lines.single) as Map<String, Object?>;
    final attributes = decoded['attributes']! as Map<String, Object?>;
    expect(attributes['authorization'], '[REDACTED]');
    expect(
      (attributes['profile']! as Map<String, Object?>)['email'],
      '[REDACTED]',
    );
    expect(decoded['stackTrace'], isNull);
  });

  test('instrumented bus propagates trace context and exports spans', () async {
    final delegate = InMemoryMessageBus();
    final spans = <PodBusSpanRecord>[];
    final bus = InstrumentedMessageBus(
      delegate: delegate,
      tracer: PodBusTracer(export: spans.add),
      transport: 'memory',
    );
    await bus.connect();
    addTearDown(bus.close);

    final received = Completer<MessageHeaders>();
    await bus.subscribe<Map<String, Object?>>(
      'lead.created',
      handler: (context, payload) async {
        expect(payload['leadId'], 42);
        received.complete(context.headers);
      },
    );

    await bus.publish('lead.created', {'leadId': 42});
    final headers = await received.future;

    expect(headers.custom[W3cTraceContext.traceParentHeader], isNotNull);
    expect(spans.map((span) => span.kind), contains(PodBusSpanKind.producer));
    expect(spans.map((span) => span.kind), contains(PodBusSpanKind.consumer));
    expect(spans.every((span) => span.status == PodBusSpanStatus.ok), isTrue);
  });
}
