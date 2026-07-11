import 'package:podbus_core/podbus_core.dart';
import 'package:test/test.dart';

void main() {
  group('MessagingCapabilities', () {
    const capabilities = MessagingCapabilities({
      MessagingCapability.durableJobs,
      MessagingCapability.deadLettering,
    });

    test('accepts supported requirements', () {
      expect(
        () => capabilities.requireAll({
          MessagingCapability.durableJobs,
          MessagingCapability.deadLettering,
        }),
        returnsNormally,
      );
    });

    test('reports every missing requirement', () {
      expect(
        () => capabilities.requireAll({
          MessagingCapability.requestReply,
          MessagingCapability.gracefulShutdown,
        }),
        throwsA(
          isA<MessagingUnsupportedException>()
              .having(
                (error) => error.message,
                'message',
                contains('requestReply'),
              )
              .having(
                (error) => error.message,
                'message',
                contains('gracefulShutdown'),
              ),
        ),
      );
    });
  });
}
