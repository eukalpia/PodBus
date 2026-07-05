import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_kafka/podbus_kafka.dart';
import 'package:test/test.dart';

void main() {
  group('KafkaMessagingConfig', () {
    test('requires brokers, client id, and group id', () {
      expect(
        () => KafkaMessagingConfig(
          brokers: const [],
          clientId: 'podbus',
          groupId: 'podbus-workers',
        ),
        throwsA(isA<MessagingConfigurationException>()),
      );

      expect(
        () => KafkaMessagingConfig(
          brokers: const ['localhost:9092'],
          clientId: '',
          groupId: 'podbus-workers',
        ),
        throwsA(isA<MessagingConfigurationException>()),
      );
    });

    test('is experimental by default', () {
      final config = KafkaMessagingConfig(
        brokers: const ['localhost:9092'],
        clientId: 'podbus',
        groupId: 'podbus-workers',
      );

      expect(config.experimental, isTrue);
    });
  });
}
