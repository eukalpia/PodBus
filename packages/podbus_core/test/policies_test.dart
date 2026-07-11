import 'package:podbus_core/podbus_core.dart';
import 'package:test/test.dart';

void main() {
  group('RetryPolicy', () {
    test('calculates exponential delays with a maximum cap', () {
      final policy = RetryPolicy(
        maxAttempts: 5,
        initialDelay: Duration(milliseconds: 100),
        maxDelay: Duration(milliseconds: 350),
        backoffMultiplier: 2,
      );

      expect(policy.delayForAttempt(1), Duration(milliseconds: 100));
      expect(policy.delayForAttempt(2), Duration(milliseconds: 200));
      expect(policy.delayForAttempt(3), Duration(milliseconds: 350));
      expect(policy.delayForAttempt(4), Duration(milliseconds: 350));
    });

    test('applies bounded deterministic jitter', () {
      final policy = RetryPolicy(
        maxAttempts: 3,
        initialDelay: const Duration(milliseconds: 100),
        maxDelay: const Duration(seconds: 1),
        jitter: 0.2,
      );

      expect(
        policy.delayForAttempt(1, randomValue: 0),
        const Duration(milliseconds: 80),
      );
      expect(
        policy.delayForAttempt(1, randomValue: 1),
        const Duration(milliseconds: 120),
      );
    });

    test('validates retry configuration', () {
      expect(
        () => RetryPolicy(
          maxAttempts: 0,
          initialDelay: Duration.zero,
          maxDelay: Duration(seconds: 1),
        ),
        throwsA(isA<MessagingConfigurationException>()),
      );
    });
  });

  group('DeadLetterPolicy', () {
    test(
      'uses disabled defaults that do not leak payload or error details',
      () {
        const policy = DeadLetterPolicy.disabled();

        expect(policy.enabled, isFalse);
        expect(policy.includeErrorDetails, isFalse);
        expect(policy.includeOriginalPayload, isFalse);
        expect(policy.destination, isNull);
      },
    );

    test('supports explicit dead-letter destination', () {
      const policy = DeadLetterPolicy(
        enabled: true,
        destination: 'jobs.dead',
        includeErrorDetails: true,
        includeOriginalPayload: true,
      );

      expect(policy.enabled, isTrue);
      expect(policy.destination, 'jobs.dead');
      expect(policy.includeErrorDetails, isTrue);
      expect(policy.includeOriginalPayload, isTrue);
    });
  });
}
