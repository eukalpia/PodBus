import 'dart:async';
import 'dart:io';

import 'package:podbus_kafka/podbus_kafka.dart';
import 'package:test/test.dart';

void main() {
  group(
    'Kafka integration',
    () {
      test('produces and consumes an event through a consumer group', () async {
        final id = DateTime.now().microsecondsSinceEpoch;
        final topic = 'podbus.tests.kafka.events.$id';
        final bus = KafkaEventBus(
          config: KafkaMessagingConfig(
            brokers: [
              Platform.environment['PODBUS_KAFKA_BROKER'] ?? 'localhost:9092',
            ],
            clientId: 'podbus-integration-$id',
            groupId: 'podbus-integration-$id',
            requestTimeout: const Duration(seconds: 5),
          ),
        );
        await bus.connect();
        addTearDown(() => bus.close(timeout: const Duration(seconds: 5)));

        final received = Completer<Map<String, Object?>>();
        await bus.subscribe<Map<String, Object?>>(
          topic,
          handler: (_, payload) async {
            received.complete(payload);
          },
        );

        await bus.publish(topic, {'leadId': 42});

        expect(await received.future.timeout(_integrationTimeout), {
          'leadId': 42,
        });
      });
    },
    tags: 'integration',
    timeout: Timeout(const Duration(seconds: 45)),
    skip: _integrationSkip,
  );
}

const _integrationTimeout = Duration(seconds: 15);

Object? get _integrationSkip {
  if (Platform.environment['PODBUS_RUN_INTEGRATION_TESTS'] == 'true') {
    return false;
  }
  return 'Set PODBUS_RUN_INTEGRATION_TESTS=true to run Docker-backed tests.';
}
