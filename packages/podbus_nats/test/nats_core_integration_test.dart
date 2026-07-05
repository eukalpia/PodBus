import 'dart:async';
import 'dart:io';

import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_nats/podbus_nats.dart';
import 'package:test/test.dart';

void main() {
  group('NATS Core integration', () {
    test('delivers published events to subscribers', () async {
      final bus = await _connectBus();
      addTearDown(() => bus.close(timeout: const Duration(seconds: 2)));

      final subject = _uniqueSubject('pubsub');
      final received = Completer<Map<String, Object?>>();

      await bus.subscribe<Map<String, Object?>>(
        subject,
        handler: (context, payload) async {
          expect(context.subject, subject);
          expect(context.headers.correlationId, 'corr-pubsub');
          received.complete(payload);
        },
      );
      await bus.healthCheck();

      await bus.publish(subject, {
        'leadId': 42,
      }, headers: MessageHeaders(correlationId: 'corr-pubsub'));

      expect(await received.future.timeout(_integrationTimeout), {
        'leadId': 42,
      });
    });

    test('load balances messages across a queue group', () async {
      final bus = await _connectBus();
      addTearDown(() => bus.close(timeout: const Duration(seconds: 2)));

      final subject = _uniqueSubject('queue');
      const messageCount = 20;
      final counts = [0, 0];
      final delivered = Completer<void>();

      void recordDelivery(int index) {
        counts[index] += 1;
        if (counts[0] + counts[1] == messageCount && !delivered.isCompleted) {
          delivered.complete();
        }
      }

      await bus.subscribe<Map<String, Object?>>(
        subject,
        queueGroup: 'lead-workers',
        handler: (_, _) async => recordDelivery(0),
      );
      await bus.subscribe<Map<String, Object?>>(
        subject,
        queueGroup: 'lead-workers',
        handler: (_, _) async => recordDelivery(1),
      );
      await bus.healthCheck();

      for (var i = 0; i < messageCount; i += 1) {
        await bus.publish(subject, {'index': i});
      }

      await delivered.future.timeout(_integrationTimeout);

      expect(counts[0] + counts[1], messageCount);
      expect(
        counts.every((count) => count > 0),
        isTrue,
        reason: 'Both queue subscribers should receive at least one message.',
      );
    });

    test('performs request reply calls', () async {
      final bus = await _connectBus();
      addTearDown(() => bus.close(timeout: const Duration(seconds: 2)));

      final subject = _uniqueSubject('request');

      await bus.subscribe<Map<String, Object?>>(
        subject,
        handler: (context, payload) async {
          await context.reply({'score': (payload['leadId'] as int) + 10});
        },
      );
      await bus.healthCheck();

      final response = await bus
          .request<Map<String, Object?>, Map<String, Object?>>(subject, {
            'leadId': 81,
          }, timeout: _integrationTimeout);

      expect(response, {'score': 91});
    });
  }, skip: _integrationSkip);
}

const _integrationTimeout = Duration(seconds: 5);

Object? get _integrationSkip {
  if (Platform.environment['PODBUS_RUN_INTEGRATION_TESTS'] == 'true') {
    return false;
  }
  return 'Set PODBUS_RUN_INTEGRATION_TESTS=true to run Docker-backed tests.';
}

Future<NatsMessageBus> _connectBus() async {
  final bus = NatsMessageBus(
    config: NatsMessagingConfig(
      servers: [
        Uri.parse(
          Platform.environment['PODBUS_NATS_URL'] ?? 'nats://localhost:4222',
        ),
      ],
      connectTimeout: const Duration(seconds: 1),
      requestTimeout: _integrationTimeout,
    ),
  );
  await bus.connect();
  return bus;
}

String _uniqueSubject(String suffix) {
  final id = DateTime.now().microsecondsSinceEpoch;
  return 'podbus.tests.nats.$suffix.$id';
}
