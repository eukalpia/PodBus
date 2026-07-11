import 'package:podbus_core/podbus_core.dart';
import 'package:test/test.dart';

void main() {
  group('MessagingConfig', () {
    test(
      'uses production-safe defaults without requiring logging or metrics backends',
      () {
        final config = MessagingConfig();

        expect(config.codec, isA<JsonMessageCodec>());
        expect(config.requestTimeout, Duration(seconds: 30));
        expect(config.logHook, isNull);
        expect(config.metricHook, isNull);
      },
    );

    test('rejects outbound messages above configured limits', () {
      final config = MessagingConfig(
        limits: const MessagingLimits(maxPayloadBytes: 4, maxHeaderBytes: 32),
      );

      expect(
        () => config.validateRawOutbound(List<int>.filled(5, 0), const {}),
        throwsA(isA<MessagingConfigurationException>()),
      );
    });

    test('passes structured log and metric events to optional hooks', () {
      final logs = <MessagingLogEvent>[];
      final metrics = <MessagingMetricEvent>[];
      final config = MessagingConfig(
        logHook: logs.add,
        metricHook: metrics.add,
      );

      config.log(
        MessagingLogLevel.info,
        'connected',
        attributes: {'transport': 'memory'},
      );
      config.recordMetric(
        'messages.published',
        value: 1,
        attributes: {'subject': 'leads.created'},
      );

      expect(logs.single.message, 'connected');
      expect(logs.single.attributes, {'transport': 'memory'});
      expect(metrics.single.name, 'messages.published');
      expect(metrics.single.value, 1);
    });
  });
}
