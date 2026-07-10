import 'dart:async';

import 'package:podbus_core/podbus_core.dart';
import 'package:test/test.dart';

void main() {
  group('InMemoryDurableJobQueue', () {
    test('processes enqueued jobs with worker context', () async {
      final queue = InMemoryDurableJobQueue();
      await queue.connect();

      final received = Completer<WelcomeEmailJob>();
      await queue.worker<WelcomeEmailJob>(
        'email.welcome',
        handler: (context, payload) async {
          expect(context.topic, 'email.welcome');
          expect(context.attempt, 1);
          received.complete(payload);
          await context.ack();
        },
      );

      await queue.enqueue('email.welcome', WelcomeEmailJob(leadId: 3));

      expect(
        await received.future.timeout(Duration(seconds: 1)),
        WelcomeEmailJob(leadId: 3),
      );
      await queue.close();
    });

    test('retries failed jobs until the handler succeeds', () async {
      final queue = InMemoryDurableJobQueue();
      await queue.connect();

      final attempts = <int>[];
      final done = Completer<void>();

      await queue.worker<WelcomeEmailJob>(
        'email.welcome',
        retryPolicy: RetryPolicy(
          maxAttempts: 2,
          initialDelay: Duration.zero,
          maxDelay: Duration.zero,
        ),
        handler: (context, _) async {
          attempts.add(context.attempt);
          if (context.attempt == 1) {
            throw StateError('temporary smtp failure');
          }
          done.complete();
          await context.ack();
        },
      );

      await queue.enqueue('email.welcome', WelcomeEmailJob(leadId: 3));
      await done.future.timeout(Duration(seconds: 1));

      expect(attempts, [1, 2]);
      await queue.close();
    });

    test('routes exhausted jobs to the configured dead-letter topic', () async {
      final queue = InMemoryDurableJobQueue();
      await queue.connect();

      final deadLetter = Completer<WelcomeEmailJob>();
      await queue.worker<WelcomeEmailJob>(
        'email.dead',
        handler: (context, payload) async {
          expect(context.headers.attempt, 2);
          deadLetter.complete(payload);
          await context.ack();
        },
      );

      await queue.worker<WelcomeEmailJob>(
        'email.welcome',
        retryPolicy: RetryPolicy(
          maxAttempts: 2,
          initialDelay: Duration.zero,
          maxDelay: Duration.zero,
        ),
        deadLetterPolicy: DeadLetterPolicy(
          enabled: true,
          destination: 'email.dead',
          includeErrorDetails: true,
          includeOriginalPayload: true,
        ),
        handler: (_, _) async {
          throw StateError('smtp unavailable');
        },
      );

      await queue.enqueue('email.welcome', WelcomeEmailJob(leadId: 3));

      expect(
        await deadLetter.future.timeout(Duration(seconds: 1)),
        WelcomeEmailJob(leadId: 3),
      );
      await queue.close();
    });

    test(
      'deduplicates enqueued jobs when an idempotency key is reused',
      () async {
        final queue = InMemoryDurableJobQueue(
          idempotencyStore: InMemoryIdempotencyStore(),
        );
        await queue.connect();

        var handled = 0;
        await queue.worker<WelcomeEmailJob>(
          'email.welcome',
          handler: (context, _) async {
            handled++;
            await context.ack();
          },
        );

        await queue.enqueue(
          'email.welcome',
          WelcomeEmailJob(leadId: 3),
          idempotencyKey: 'email:3',
        );
        await queue.enqueue(
          'email.welcome',
          WelcomeEmailJob(leadId: 3),
          idempotencyKey: 'email:3',
        );
        await Future<void>.delayed(Duration(milliseconds: 10));

        expect(handled, 1);
        await queue.close();
      },
    );
  });
}

final class WelcomeEmailJob {
  const WelcomeEmailJob({required this.leadId});

  final int leadId;

  @override
  bool operator ==(Object other) {
    return other is WelcomeEmailJob && other.leadId == leadId;
  }

  @override
  int get hashCode => leadId.hashCode;
}
