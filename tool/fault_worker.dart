import 'dart:async';
import 'dart:io';

import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_nats/podbus_nats.dart';
import 'package:podbus_rabbitmq/podbus_rabbitmq.dart';

import 'src/fault_harness.dart';

Future<void> main(List<String> arguments) async {
  final options = _Options(arguments);
  final transport = options.required('transport');
  final mode = options.required('mode');
  final topic = options.required('topic');
  final durable = options.required('durable');
  final workerId = options.required('worker-id');
  final journal = File(options.required('journal'));
  final done = Completer<void>();

  final DurableJobQueue queue = switch (transport) {
    'nats' => _natsQueue(options, topic),
    'rabbitmq' => _rabbitQueue(options),
    _ => throw ArgumentError.value(
      transport,
      'transport',
      'Expected nats or rabbitmq',
    ),
  };

  await queue.connect();
  await queue.worker<Map<String, Object?>>(
    topic,
    durableName: durable,
    retryPolicy: RetryPolicy(
      maxAttempts: 5,
      initialDelay: Duration.zero,
      maxDelay: Duration.zero,
      jitter: 0,
    ),
    handler: (context, payload) async {
      await appendJsonLine(journal, {
        'event': mode == 'crash' ? 'side-effect-before-crash' : 'delivery',
        'transport': transport,
        'workerId': workerId,
        'id': payload['id'],
        'attempt': context.attempt,
        'pid': pid,
        'at': DateTime.now().toUtc().toIso8601String(),
      });
      if (mode == 'crash') {
        Process.killPid(pid, ProcessSignal.sigkill);
        await Completer<void>().future;
      }
      if (!done.isCompleted) {
        done.complete();
      }
    },
  );

  await appendJsonLine(journal, {
    'event': 'ready',
    'transport': transport,
    'workerId': workerId,
    'pid': pid,
    'at': DateTime.now().toUtc().toIso8601String(),
  });

  if (mode == 'crash') {
    await Completer<void>().future;
  }
  if (mode != 'ack') {
    throw ArgumentError.value(mode, 'mode', 'Expected crash or ack');
  }
  await done.future.timeout(const Duration(seconds: 30));
  await Future<void>.delayed(const Duration(milliseconds: 250));
  await queue.close(timeout: const Duration(seconds: 5));
}

NatsJetStreamJobQueue _natsQueue(_Options options, String topic) {
  final stream = options.required('stream');
  final ackWaitMs = options.intValue('ack-wait-ms', fallback: 500);
  return NatsJetStreamJobQueue(
    config: NatsMessagingConfig(
      servers: [Uri.parse(options.required('nats-url'))],
      connectTimeout: const Duration(seconds: 2),
      requestTimeout: const Duration(seconds: 2),
      jetStream: NatsJetStreamConfig(
        enabled: true,
        streamName: stream,
        subjects: [topic],
        storage: NatsJetStreamStorage.file,
        consumerConfig: NatsJetStreamConsumerConfig(
          ackWait: Duration(milliseconds: ackWaitMs),
          maxDeliver: 5,
          maxAckPending: 32,
        ),
      ),
    ),
    fetchTimeout: const Duration(milliseconds: 100),
  );
}

RabbitMqMessageBus _rabbitQueue(_Options options) {
  return RabbitMqMessageBus(
    config: RabbitMqMessagingConfig(
      uri: Uri.parse(options.required('rabbit-url')),
      exchange: options.required('exchange'),
      deadLetterExchange: options.required('dead-exchange'),
      publisherConfirmTimeout: const Duration(seconds: 2),
      maxConnectionAttempts: 1,
      reconnectWaitTime: const Duration(milliseconds: 100),
      connectionName: 'podbus-fault-${options.required('worker-id')}',
    ),
  );
}

final class _Options {
  _Options(List<String> arguments) {
    for (final argument in arguments) {
      if (!argument.startsWith('--') || !argument.contains('=')) {
        throw FormatException('Expected --name=value, got "$argument".');
      }
      final separator = argument.indexOf('=');
      _values[argument.substring(2, separator)] = argument.substring(
        separator + 1,
      );
    }
  }

  final Map<String, String> _values = {};

  String required(String name) {
    final value = _values[name];
    if (value == null || value.isEmpty) {
      throw ArgumentError('Missing --$name=value.');
    }
    return value;
  }

  int intValue(String name, {required int fallback}) {
    final value = _values[name];
    return value == null ? fallback : int.parse(value);
  }
}
