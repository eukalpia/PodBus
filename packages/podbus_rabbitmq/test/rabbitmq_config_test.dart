import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_rabbitmq/podbus_rabbitmq.dart';
import 'package:test/test.dart';

void main() {
  group('RabbitMqMessagingConfig', () {
    test('validates exchange names and prefetch count', () {
      expect(
        () => RabbitMqMessagingConfig(
          uri: Uri.parse('amqp://localhost:5672'),
          exchange: '',
          deadLetterExchange: 'podbus.dead',
        ),
        throwsA(isA<MessagingConfigurationException>()),
      );

      expect(
        () => RabbitMqMessagingConfig(
          uri: Uri.parse('amqp://localhost:5672'),
          exchange: 'podbus',
          deadLetterExchange: 'podbus.dead',
          prefetchCount: 0,
        ),
        throwsA(isA<MessagingConfigurationException>()),
      );
    });
  });
}
