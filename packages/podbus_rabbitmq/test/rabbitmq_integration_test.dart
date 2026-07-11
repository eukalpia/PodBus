import 'dart:async';
import 'dart:io';

import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_rabbitmq/podbus_rabbitmq.dart';
import 'package:test/test.dart';

void main() {
  group(
    'RabbitMQ integration',
    () {
      test('publishes and consumes messages through a bound queue', () async {
        final id = DateTime.now().microsecondsSinceEpoch;
        final subject = 'podbus.tests.rabbit.events.$id';
        final bus = RabbitMqMessageBus(
          config: _config(
            exchange: 'podbus.tests.events.$id',
            deadLetterExchange: 'podbus.tests.dead.$id',
          ),
        );
        await bus.connect();
        addTearDown(() => bus.close(timeout: const Duration(seconds: 3)));

        final received = Completer<Map<String, Object?>>();
        await bus.subscribe<Map<String, Object?>>(
          subject,
          queueGroup: 'integration_workers',
          handler: (context, payload) async {
            expect(context.subject, subject);
            received.complete(payload);
          },
        );

        await bus.publish(subject, {'leadId': 42});

        expect(await received.future.timeout(_integrationTimeout), {
          'leadId': 42,
        });
      });

      test('publishes failed jobs to a dead-letter exchange', () async {
        final id = DateTime.now().microsecondsSinceEpoch;
        final topic = 'podbus.tests.rabbit.jobs.$id';
        final deadTopic = '$topic.dead';
        final deadExchange = 'podbus.tests.dead.$id';
        final bus = RabbitMqMessageBus(
          config: _config(
            exchange: 'podbus.tests.jobs.$id',
            deadLetterExchange: deadExchange,
          ),
        );
        final deadBus = RabbitMqMessageBus(
          config: _config(
            exchange: deadExchange,
            deadLetterExchange: deadExchange,
          ),
        );
        await bus.connect();
        await deadBus.connect();
        addTearDown(() => bus.close(timeout: const Duration(seconds: 3)));
        addTearDown(() => deadBus.close(timeout: const Duration(seconds: 3)));

        final deadLetter = Completer<Map<String, Object?>>();
        await deadBus.subscribe<Map<String, Object?>>(
          deadTopic,
          queueGroup: 'dead_letter_workers',
          handler: (_, payload) async {
            deadLetter.complete(payload);
          },
        );

        await bus.worker<Map<String, Object?>>(
          topic,
          retryPolicy: RetryPolicy(
            maxAttempts: 1,
            initialDelay: Duration.zero,
            maxDelay: Duration.zero,
          ),
          deadLetterPolicy: DeadLetterPolicy(
            enabled: true,
            destination: deadTopic,
            includeErrorDetails: true,
            includeOriginalPayload: true,
          ),
          handler: (_, _) async {
            throw StateError('smtp unavailable');
          },
        );
        await bus.enqueue(topic, {'leadId': 42});

        expect(await deadLetter.future.timeout(_integrationTimeout), {
          'leadId': 42,
        });
      });
    },
    tags: 'integration',
    timeout: Timeout(const Duration(seconds: 40)),
    skip: _integrationSkip,
  );
}

const _integrationTimeout = Duration(seconds: 8);

Object? get _integrationSkip {
  if (Platform.environment['PODBUS_RUN_INTEGRATION_TESTS'] == 'true') {
    return false;
  }
  return 'Set PODBUS_RUN_INTEGRATION_TESTS=true to run Docker-backed tests.';
}

RabbitMqMessagingConfig _config({
  required String exchange,
  required String deadLetterExchange,
}) {
  return RabbitMqMessagingConfig(
    uri: Uri.parse(
      Platform.environment['PODBUS_RABBITMQ_URL'] ??
          'amqp://guest:guest@localhost:5672',
    ),
    exchange: exchange,
    deadLetterExchange: deadLetterExchange,
    connectTimeout: const Duration(seconds: 1),
  );
}
