import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_nats/podbus_nats.dart';
import 'package:test/test.dart';

void main() {
  group('NatsMessagingConfig', () {
    test('requires at least one server', () {
      expect(
        () => NatsMessagingConfig(servers: const []),
        throwsA(isA<MessagingConfigurationException>()),
      );
    });

    test('keeps JetStream disabled unless configured', () {
      final config = NatsMessagingConfig(
        servers: [Uri.parse('nats://localhost:4222')],
      );

      expect(config.servers.single, Uri.parse('nats://localhost:4222'));
      expect(config.jetStream, isNull);
      expect(config.requestTimeout, Duration(seconds: 30));
    });
  });
}
