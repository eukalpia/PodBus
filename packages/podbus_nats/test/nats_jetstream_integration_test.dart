import 'dart:async';
import 'dart:io';

import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_nats/podbus_nats.dart';
import 'package:test/test.dart';

void main() {
  group('NATS JetStream integration', () {
    test(
      'delivers a durable job that was enqueued before the worker starts',
      () async {
        final id = DateTime.now().microsecondsSinceEpoch;
        final topic = 'podbus.tests.jetstream.jobs.$id';
        final queue = NatsJetStreamJobQueue(
          config: _config(streamName: 'PODBUS_TESTS_$id', subjects: [topic]),
        );
        await queue.connect();
        addTearDown(() => queue.close(timeout: const Duration(seconds: 3)));

        await queue.enqueue(topic, {
          'leadId': 42,
        }, headers: MessageHeaders(correlationId: 'corr-jetstream'));

        final received = Completer<Map<String, Object?>>();
        await queue.worker<Map<String, Object?>>(
          topic,
          durableName: 'email_workers',
          retryPolicy: RetryPolicy(
            maxAttempts: 1,
            initialDelay: Duration.zero,
            maxDelay: Duration.zero,
          ),
          handler: (context, payload) async {
            expect(context.topic, topic);
            expect(context.headers.correlationId, 'corr-jetstream');
            received.complete(payload);
          },
        );

        expect(await received.future.timeout(_integrationTimeout), {
          'leadId': 42,
        });
      },
    );
  }, skip: _integrationSkip);
}

const _integrationTimeout = Duration(seconds: 5);

Object? get _integrationSkip {
  if (Platform.environment['PODBUS_RUN_INTEGRATION_TESTS'] == 'true') {
    return false;
  }
  return 'Set PODBUS_RUN_INTEGRATION_TESTS=true to run Docker-backed tests.';
}

NatsMessagingConfig _config({
  required String streamName,
  required List<String> subjects,
}) {
  return NatsMessagingConfig(
    servers: [
      Uri.parse(
        Platform.environment['PODBUS_NATS_URL'] ?? 'nats://localhost:4222',
      ),
    ],
    connectTimeout: const Duration(seconds: 1),
    requestTimeout: _integrationTimeout,
    jetStream: NatsJetStreamConfig(
      enabled: true,
      streamName: streamName,
      subjects: subjects,
      storage: NatsJetStreamStorage.memory,
    ),
  );
}
