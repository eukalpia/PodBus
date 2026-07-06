import 'package:test/test.dart';

import '../tool/stress_transports.dart';

void main() {
  group('StressOptions', () {
    test('uses the four benchmark modes by default', () {
      final options = StressOptions.fromEnvironment(const {});

      expect(options.modes, [
        StressMode.fast,
        StressMode.durable,
        StressMode.worker,
        StressMode.failure,
      ]);
    });

    test('parses benchmark matrix parameters from environment', () {
      final options = StressOptions.fromEnvironment(const {
        'PODBUS_STRESS_PAYLOAD_SIZES': '256,1024,10240',
        'PODBUS_STRESS_CONSUMERS': '1,4,16',
        'PODBUS_STRESS_PRODUCERS': '1,4,16',
        'PODBUS_STRESS_HANDLER_SLEEP_MS': '5,50',
      });

      expect(options.payloadSizes, [256, 1024, 10240]);
      expect(options.consumerCounts, [1, 4, 16]);
      expect(options.producerCounts, [1, 4, 16]);
      expect(options.handlerSleeps, [
        const Duration(milliseconds: 5),
        const Duration(milliseconds: 50),
      ]);
    });

    test('resolves broker endpoints from transport configuration', () {
      expect(
        brokerEndpointForTransport('nats', const {
          'PODBUS_NATS_URL': 'nats://nats.example.test:4223',
        }),
        const BrokerEndpoint('nats.example.test', 4223),
      );
      expect(
        brokerEndpointForTransport('rabbitmq', const {
          'PODBUS_RABBITMQ_URL': 'amqp://guest:guest@mq.test:5673',
        }),
        const BrokerEndpoint('mq.test', 5673),
      );
      expect(
        brokerEndpointForTransport('kafka', const {
          'PODBUS_KAFKA_BROKER': 'kafka.test:19092',
        }),
        const BrokerEndpoint('kafka.test', 19092),
      );
    });
  });
}
