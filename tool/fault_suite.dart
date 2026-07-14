import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math' as math;

import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_nats/podbus_nats.dart';
import 'package:podbus_rabbitmq/podbus_rabbitmq.dart';
import 'package:podbus_rabbitmq/src/rabbitmq_adapter.dart';

import 'src/fault_harness.dart';

Future<void> main(List<String> arguments) async {
  final options = _Options(arguments);
  final profile = options.value('profile', fallback: 'smoke');
  if (profile != 'smoke' && profile != 'full') {
    throw ArgumentError.value(profile, 'profile', 'Expected smoke or full.');
  }
  final manageDocker = options.boolValue('manage-docker', fallback: false);
  final report = File(
    options.value('report', fallback: 'test-results/fault-suite.json'),
  );
  final environment = FaultEnvironment(
    composeFile: options.value(
      'compose-file',
      fallback: 'docker-compose.integration.yaml',
    ),
  );
  final runId = DateTime.now().microsecondsSinceEpoch.toString();
  final results = <Map<String, Object?>>[];
  final failures = <String>[];

  if (manageDocker) {
    await environment.startServices(['nats', 'rabbitmq', 'toxiproxy']);
  }

  try {
    await environment.waitForService('nats');
    await environment.waitForService('rabbitmq');
    await environment.waitForService('toxiproxy');
    await _resetProxies(environment);

    final allScenarios = <String, Future<Map<String, Object?>> Function()>{
      'nats-tcp-partition': () => _natsTcpPartition(environment, runId),
      'rabbitmq-tcp-partition': () => _rabbitTcpPartition(environment, runId),
      'rabbitmq-channel-failures': () =>
          _rabbitChannelFailures(environment, runId),
      'nats-crash-before-ack': () => _natsCrashBeforeAck(runId),
      'rabbitmq-crash-before-ack': () => _rabbitCrashBeforeAck(runId),
      'multiple-replicas': () => _multipleReplicas(runId, profile: profile),
      'nats-broker-stop-before-confirm': () =>
          _natsStopBeforeConfirm(environment, runId),
      'rabbitmq-broker-stop-before-confirm': () =>
          _rabbitStopBeforeConfirm(environment, runId),
      'nats-shutdown-during-dlq-ack': () =>
          _natsShutdownDuringDlqAck(environment, runId),
      'rabbitmq-shutdown-during-retry-confirm': () =>
          _rabbitShutdownDuringRetryConfirm(environment, runId),
      'rabbitmq-shutdown-during-dlq-confirm': () =>
          _rabbitShutdownDuringDlqConfirm(environment, runId),
      'slow-consumers': () => _slowConsumers(runId, profile: profile),
    };
    final requested = options.csvValue('scenario');
    final scenarios = requested.isEmpty
        ? allScenarios
        : <String, Future<Map<String, Object?>> Function()>{
            for (final name in requested)
              name:
                  allScenarios[name] ??
                  (throw ArgumentError.value(
                    name,
                    'scenario',
                    'Unknown scenario. Available: ${allScenarios.keys.join(', ')}',
                  )),
          };

    for (final entry in scenarios.entries) {
      final startedAt = DateTime.now().toUtc();
      stdout.writeln('::group::${entry.key}');
      try {
        final metrics = await entry.value();
        final finishedAt = DateTime.now().toUtc();
        results.add({
          'name': entry.key,
          'success': true,
          'startedAt': startedAt.toIso8601String(),
          'finishedAt': finishedAt.toIso8601String(),
          'durationMs': finishedAt.difference(startedAt).inMilliseconds,
          'metrics': metrics,
        });
        stdout.writeln('PASS ${entry.key}: ${jsonEncode(metrics)}');
      } on Object catch (error, stackTrace) {
        final finishedAt = DateTime.now().toUtc();
        failures.add('${entry.key}: $error');
        results.add({
          'name': entry.key,
          'success': false,
          'startedAt': startedAt.toIso8601String(),
          'finishedAt': finishedAt.toIso8601String(),
          'durationMs': finishedAt.difference(startedAt).inMilliseconds,
          'error': error.toString(),
          'stackTrace': stackTrace.toString(),
        });
        stderr.writeln('FAIL ${entry.key}: $error\n$stackTrace');
      } finally {
        stdout.writeln('::endgroup::');
        await _restoreEnvironment(environment);
      }
    }
  } finally {
    await report.parent.create(recursive: true);
    await report.writeAsString(
      const JsonEncoder.withIndent('  ').convert({
        'runId': runId,
        'profile': profile,
        'generatedAt': DateTime.now().toUtc().toIso8601String(),
        'success': failures.isEmpty,
        'results': results,
      }),
      flush: true,
    );
    if (manageDocker) {
      await environment.stopServices(removeVolumes: true);
    }
  }

  if (failures.isNotEmpty) {
    throw StateError('Fault suite failed:\n${failures.join('\n')}');
  }
}

const _natsProxyName = 'podbus-nats';
const _rabbitProxyName = 'podbus-rabbitmq';
const _natsProxyUrl = 'nats://127.0.0.1:14222';
const _rabbitProxyUrl = 'amqp://guest:guest@127.0.0.1:15670';
const _directNatsUrl = 'nats://127.0.0.1:4222';
const _directRabbitUrl = 'amqp://guest:guest@127.0.0.1:5672';
const _scenarioTimeout = Duration(seconds: 45);

Future<void> _resetProxies(FaultEnvironment environment) async {
  await environment.toxiproxy.reset();
  await environment.toxiproxy.createProxy(
    name: _natsProxyName,
    listen: '0.0.0.0:14222',
    upstream: 'nats:4222',
  );
  await environment.toxiproxy.createProxy(
    name: _rabbitProxyName,
    listen: '0.0.0.0:15670',
    upstream: 'rabbitmq:5672',
  );
  await environment.waitForPort('127.0.0.1', 14222);
  await environment.waitForPort('127.0.0.1', 15670);
}

Future<void> _restoreEnvironment(FaultEnvironment environment) async {
  for (final service in ['nats', 'rabbitmq']) {
    await environment.startService(service).catchError((_) {});
    await environment.waitForService(service).catchError((_) {});
  }
  await environment.toxiproxy.reset().catchError((_) {});
  await environment.toxiproxy
      .setEnabled(_natsProxyName, true)
      .catchError((_) {});
  await environment.toxiproxy
      .setEnabled(_rabbitProxyName, true)
      .catchError((_) {});
}

ReconnectPolicy _faultReconnectPolicy() => const ReconnectPolicy(
  maxAttempts: 30,
  initialDelay: Duration(milliseconds: 100),
  maxDelay: Duration(seconds: 1),
  jitter: 0.1,
  recoveryTimeout: Duration(seconds: 35),
  healthCheckInterval: Duration(milliseconds: 150),
  healthCheckTimeout: Duration(milliseconds: 500),
);

NatsMessagingConfig _natsConfig({
  required String url,
  required String stream,
  required String topic,
  Duration ackWait = const Duration(seconds: 2),
}) {
  return NatsMessagingConfig(
    servers: [Uri.parse(url)],
    connectTimeout: const Duration(seconds: 1),
    requestTimeout: const Duration(seconds: 1),
    jetStream: NatsJetStreamConfig(
      enabled: true,
      streamName: stream,
      subjects: [topic],
      storage: NatsJetStreamStorage.file,
      maxAge: const Duration(hours: 1),
      consumerConfig: NatsJetStreamConsumerConfig(
        ackWait: ackWait,
        maxDeliver: 20,
        maxAckPending: 2048,
      ),
    ),
  );
}

RabbitMqMessagingConfig _rabbitConfig({
  required String url,
  required String exchange,
  required String deadExchange,
  required String connectionName,
  Duration confirmTimeout = const Duration(seconds: 2),
}) {
  return RabbitMqMessagingConfig(
    uri: Uri.parse(url),
    exchange: exchange,
    deadLetterExchange: deadExchange,
    connectTimeout: const Duration(seconds: 1),
    publisherConfirmTimeout: confirmTimeout,
    maxConnectionAttempts: 1,
    reconnectWaitTime: const Duration(milliseconds: 100),
    connectionName: connectionName,
    prefetchCount: 128,
  );
}

Future<Map<String, Object?>> _natsTcpPartition(
  FaultEnvironment environment,
  String runId,
) async {
  final topic = 'podbus.fault.nats.partition.$runId';
  final stream = 'PODBUS_FAULT_NATS_PARTITION_$runId';
  var factories = 0;
  final deliveries = <String>[];
  final delivered = Completer<void>();
  final queue = ResilientDurableJobQueue(
    factory: () {
      factories += 1;
      return NatsJetStreamJobQueue(
        config: _natsConfig(url: _natsProxyUrl, stream: stream, topic: topic),
        fetchTimeout: const Duration(milliseconds: 100),
        fetchBatchSize: 16,
      );
    },
    policy: _faultReconnectPolicy(),
  );

  try {
    await queue.connect();
    await queue.worker<Map<String, Object?>>(
      topic,
      durableName: 'partition-workers',
      handler: (_, payload) async {
        deliveries.add(payload['id']! as String);
        if (payload['id'] == 'after' && !delivered.isCompleted) {
          delivered.complete();
        }
      },
    );
    await queue.enqueue(topic, {
      'id': 'before',
    }, idempotencyKey: '$runId-before');
    await waitUntil(
      () => deliveries.contains('before'),
      timeout: _scenarioTimeout,
      description: 'baseline NATS delivery',
    );

    await environment.toxiproxy.setEnabled(_natsProxyName, false);
    final publish = queue.enqueue(topic, {
      'id': 'after',
    }, idempotencyKey: '$runId-after');
    await Future<void>.delayed(const Duration(seconds: 2));
    await environment.toxiproxy.setEnabled(_natsProxyName, true);
    await environment.waitForPort('127.0.0.1', 14222);
    await publish.timeout(_scenarioTimeout);
    await delivered.future.timeout(_scenarioTimeout);

    if (factories < 2) {
      throw StateError('NATS partition did not recreate the delegate.');
    }
    return {
      'factoryCalls': factories,
      'deliveries': deliveries,
      'duplicateCount': deliveries.length - deliveries.toSet().length,
    };
  } finally {
    await environment.toxiproxy
        .setEnabled(_natsProxyName, true)
        .catchError((_) {});
    await queue.close(timeout: const Duration(seconds: 5)).catchError((_) {});
  }
}

Future<Map<String, Object?>> _rabbitTcpPartition(
  FaultEnvironment environment,
  String runId,
) async {
  final topic = 'podbus.fault.rabbit.partition.$runId';
  final exchange = 'podbus.fault.rabbit.partition.exchange.$runId';
  final deadExchange = '$exchange.dead';
  var factories = 0;
  final deliveries = <String>[];
  final delivered = Completer<void>();
  final queue = ResilientDurableJobQueue(
    factory: () {
      factories += 1;
      return RabbitMqMessageBus(
        config: _rabbitConfig(
          url: _rabbitProxyUrl,
          exchange: exchange,
          deadExchange: deadExchange,
          connectionName: 'fault-rabbit-partition-$factories',
        ),
      );
    },
    policy: _faultReconnectPolicy(),
  );

  try {
    await queue.connect();
    await queue.worker<Map<String, Object?>>(
      topic,
      durableName: 'fault-rabbit-partition-$runId',
      handler: (_, payload) async {
        deliveries.add(payload['id']! as String);
        if (payload['id'] == 'after' && !delivered.isCompleted) {
          delivered.complete();
        }
      },
    );
    await queue.enqueue(topic, {'id': 'before'});
    await waitUntil(
      () => deliveries.contains('before'),
      timeout: _scenarioTimeout,
      description: 'baseline RabbitMQ delivery',
    );

    await environment.toxiproxy.setEnabled(_rabbitProxyName, false);
    final publish = queue.enqueue(topic, {'id': 'after'});
    await Future<void>.delayed(const Duration(seconds: 2));
    await environment.toxiproxy.setEnabled(_rabbitProxyName, true);
    await environment.waitForPort('127.0.0.1', 15670);
    await publish.timeout(_scenarioTimeout);
    await delivered.future.timeout(_scenarioTimeout);

    if (factories < 2) {
      throw StateError('RabbitMQ partition did not recreate the delegate.');
    }
    return {
      'factoryCalls': factories,
      'deliveries': deliveries,
      'duplicateCount': deliveries.length - deliveries.toSet().length,
    };
  } finally {
    await environment.toxiproxy
        .setEnabled(_rabbitProxyName, true)
        .catchError((_) {});
    await queue.close(timeout: const Duration(seconds: 5)).catchError((_) {});
  }
}

Future<Map<String, Object?>> _rabbitChannelFailures(
  FaultEnvironment environment,
  String runId,
) async {
  final topic = 'podbus.fault.rabbit.channels.$runId';
  final exchange = 'podbus.fault.rabbit.channels.exchange.$runId';
  final deadExchange = '$exchange.dead';
  final durable = 'fault-rabbit-channels-$runId';
  final adapters = <DartRabbitMqAdapter>[];
  var factories = 0;
  final received = <String>[];
  final queue = ResilientDurableJobQueue(
    factory: () {
      factories += 1;
      final adapter = DartRabbitMqAdapter();
      adapters.add(adapter);
      return RabbitMqMessageBus(
        config: _rabbitConfig(
          url: _rabbitProxyUrl,
          exchange: exchange,
          deadExchange: deadExchange,
          connectionName: 'fault-rabbit-channel-$factories',
        ),
        adapter: adapter,
      );
    },
    policy: _faultReconnectPolicy(),
  );

  try {
    await queue.connect();
    await queue.worker<Map<String, Object?>>(
      topic,
      durableName: durable,
      handler: (_, payload) async => received.add(payload['id']! as String),
    );

    var publisherFaulted = false;
    try {
      await adapters.last.declareExchange(name: exchange, durable: false);
    } on Object {
      publisherFaulted = true;
    }
    if (!publisherFaulted) {
      throw StateError('Publisher channel mismatch unexpectedly succeeded.');
    }
    await waitUntil(
      () => !adapters.first.isConnected,
      timeout: const Duration(seconds: 10),
      description: 'publisher channel error propagation',
    );
    await waitUntil(
      () => factories >= 2,
      timeout: _scenarioTimeout,
      description: 'publisher channel recovery',
    );
    await queue.enqueue(topic, {'id': 'after-publisher-fault'});
    await waitUntil(
      () => received.contains('after-publisher-fault'),
      timeout: _scenarioTimeout,
      description: 'delivery after publisher channel recovery',
    );

    final activeAdapter = adapters.last;
    var consumerFaulted = false;
    try {
      await activeAdapter.declareQueue(name: durable, durable: false);
    } on Object {
      consumerFaulted = true;
    }
    if (!consumerFaulted) {
      throw StateError('Consumer channel mismatch unexpectedly succeeded.');
    }
    await waitUntil(
      () => !activeAdapter.isConnected,
      timeout: const Duration(seconds: 10),
      description: 'consumer channel error propagation',
    );
    await waitUntil(
      () => factories >= 3,
      timeout: _scenarioTimeout,
      description: 'consumer channel recovery',
    );
    await queue.enqueue(topic, {'id': 'after-consumer-fault'});
    await waitUntil(
      () => received.contains('after-consumer-fault'),
      timeout: _scenarioTimeout,
      description: 'delivery after consumer channel recovery',
    );

    return {'factoryCalls': factories, 'deliveries': received};
  } finally {
    await queue.close(timeout: const Duration(seconds: 5)).catchError((_) {});
    await Future.wait([
      for (final adapter in adapters)
        adapter.close().timeout(const Duration(seconds: 5)).catchError((_) {}),
    ]);
  }
}

Future<Map<String, Object?>> _natsCrashBeforeAck(String runId) async {
  final topic = 'podbus.fault.nats.crash.$runId';
  final stream = 'PODBUS_FAULT_NATS_CRASH_$runId';
  final durable = 'fault-nats-crash-$runId';
  final journal = File('test-results/fault-nats-crash-$runId.jsonl');
  await journal.parent.create(recursive: true);
  await journal.writeAsString('');

  final producer = NatsJetStreamJobQueue(
    config: _natsConfig(
      url: _directNatsUrl,
      stream: stream,
      topic: topic,
      ackWait: const Duration(milliseconds: 700),
    ),
  );
  await producer.connect();
  await producer.enqueue(topic, {'id': 'crash-job'}, idempotencyKey: runId);
  await producer.close();

  final crash = await _startFaultWorker([
    '--transport=nats',
    '--mode=crash',
    '--topic=$topic',
    '--durable=$durable',
    '--worker-id=crash',
    '--journal=${journal.absolute.path}',
    '--stream=$stream',
    '--nats-url=$_directNatsUrl',
    '--ack-wait-ms=700',
  ]);
  await waitUntil(
    () => readJsonLines(
      journal,
    ).any((event) => event['event'] == 'side-effect-before-crash'),
    timeout: _scenarioTimeout,
    description: 'NATS side effect before process crash',
  );
  final crashExit = await crash.exitCode.timeout(_scenarioTimeout);
  if (crashExit == 0) {
    throw StateError('NATS crash worker exited successfully instead of dying.');
  }

  await Future<void>.delayed(const Duration(seconds: 2));
  final ack = await _startFaultWorker([
    '--transport=nats',
    '--mode=ack',
    '--topic=$topic',
    '--durable=$durable',
    '--worker-id=ack',
    '--journal=${journal.absolute.path}',
    '--stream=$stream',
    '--nats-url=$_directNatsUrl',
    '--ack-wait-ms=700',
  ]);
  final ackExit = await ack.exitCode.timeout(_scenarioTimeout);
  if (ackExit != 0) {
    throw StateError('NATS ack worker failed with exit code $ackExit.');
  }
  final events = readJsonLines(journal);
  final redelivery = events.firstWhere((event) => event['event'] == 'delivery');
  final attempt = redelivery['attempt'] as int;
  if (attempt < 2) {
    throw StateError('Expected NATS redelivery attempt >= 2, got $attempt.');
  }
  return {
    'crashExitCode': crashExit,
    'redeliveryAttempt': attempt,
    'events': events,
  };
}

Future<Map<String, Object?>> _rabbitCrashBeforeAck(String runId) async {
  final topic = 'podbus.fault.rabbit.crash.$runId';
  final exchange = 'podbus.fault.rabbit.crash.exchange.$runId';
  final deadExchange = '$exchange.dead';
  final durable = 'fault-rabbit-crash-$runId';
  final journal = File('test-results/fault-rabbit-crash-$runId.jsonl');
  await journal.parent.create(recursive: true);
  await journal.writeAsString('');

  final producer = RabbitMqMessageBus(
    config: _rabbitConfig(
      url: _directRabbitUrl,
      exchange: exchange,
      deadExchange: deadExchange,
      connectionName: 'fault-rabbit-crash-producer',
    ),
  );
  await producer.connect();
  await producer
      .worker<Map<String, Object?>>(
        topic,
        durableName: durable,
        handler: (_, _) async {},
      )
      .then((worker) => worker.close());
  await producer.enqueue(topic, {'id': 'crash-job'});
  await producer.close();

  final common = [
    '--transport=rabbitmq',
    '--topic=$topic',
    '--durable=$durable',
    '--journal=${journal.absolute.path}',
    '--rabbit-url=$_directRabbitUrl',
    '--exchange=$exchange',
    '--dead-exchange=$deadExchange',
  ];
  final crash = await _startFaultWorker([
    ...common,
    '--mode=crash',
    '--worker-id=crash',
  ]);
  await waitUntil(
    () => readJsonLines(
      journal,
    ).any((event) => event['event'] == 'side-effect-before-crash'),
    timeout: _scenarioTimeout,
    description: 'RabbitMQ side effect before process crash',
  );
  final crashExit = await crash.exitCode.timeout(_scenarioTimeout);
  if (crashExit == 0) {
    throw StateError(
      'RabbitMQ crash worker exited successfully instead of dying.',
    );
  }
  await Future<void>.delayed(const Duration(seconds: 1));
  final ack = await _startFaultWorker([
    ...common,
    '--mode=ack',
    '--worker-id=ack',
  ]);
  final ackExit = await ack.exitCode.timeout(_scenarioTimeout);
  if (ackExit != 0) {
    throw StateError('RabbitMQ ack worker failed with exit code $ackExit.');
  }
  final events = readJsonLines(journal);
  final crashIds = events
      .where((event) => event['event'] == 'side-effect-before-crash')
      .map((event) => event['id'])
      .toList();
  final ackIds = events
      .where((event) => event['event'] == 'delivery')
      .map((event) => event['id'])
      .toList();
  if (!crashIds.contains('crash-job') || !ackIds.contains('crash-job')) {
    throw StateError('RabbitMQ did not redeliver the unacknowledged job.');
  }
  return {
    'crashExitCode': crashExit,
    'duplicateSideEffectObserved': true,
    'events': events,
  };
}

Future<Process> _startFaultWorker(List<String> arguments) async {
  final process = await Process.start(Platform.resolvedExecutable, [
    'run',
    'tool/fault_worker.dart',
    ...arguments,
  ]);
  unawaited(process.stdout.transform(utf8.decoder).forEach(stdout.write));
  unawaited(process.stderr.transform(utf8.decoder).forEach(stderr.write));
  return process;
}

Future<Map<String, Object?>> _multipleReplicas(
  String runId, {
  required String profile,
}) async {
  final count = profile == 'full' ? 500 : 100;
  final nats = await _natsReplicaScenario(runId, count);
  final rabbit = await _rabbitReplicaScenario(runId, count);
  return {'nats': nats, 'rabbitmq': rabbit};
}

Future<Map<String, Object?>> _natsReplicaScenario(
  String runId,
  int count,
) async {
  final topic = 'podbus.fault.nats.replicas.$runId';
  final stream = 'PODBUS_FAULT_NATS_REPLICAS_$runId';
  final config = _natsConfig(url: _directNatsUrl, stream: stream, topic: topic);
  final first = NatsJetStreamJobQueue(config: config, fetchBatchSize: 32);
  final second = NatsJetStreamJobQueue(config: config, fetchBatchSize: 32);
  final unique = <int>{};
  final duplicates = <int>[];
  final workerCounts = <String, int>{'one': 0, 'two': 0};
  final done = Completer<void>();

  Future<void> handle(String worker, Map<String, Object?> payload) async {
    await Future<void>.delayed(const Duration(milliseconds: 3));
    final id = payload['id']! as int;
    workerCounts[worker] = workerCounts[worker]! + 1;
    if (!unique.add(id)) duplicates.add(id);
    if (unique.length == count && !done.isCompleted) done.complete();
  }

  try {
    await first.connect();
    await second.connect();
    await first.worker<Map<String, Object?>>(
      topic,
      durableName: 'replica-workers',
      concurrency: 4,
      handler: (_, payload) => handle('one', payload),
    );
    await second.worker<Map<String, Object?>>(
      topic,
      durableName: 'replica-workers',
      concurrency: 4,
      handler: (_, payload) => handle('two', payload),
    );
    for (var id = 0; id < count; id += 1) {
      await first.enqueue(topic, {'id': id}, idempotencyKey: '$runId-nats-$id');
    }
    await done.future.timeout(_scenarioTimeout);
    if (workerCounts.values.where((value) => value > 0).length < 2) {
      throw StateError('NATS jobs were not consumed by both replicas.');
    }
    if (duplicates.isNotEmpty) {
      throw StateError('NATS replica test observed duplicates: $duplicates');
    }
    return {
      'published': count,
      'unique': unique.length,
      'duplicates': duplicates.length,
      'workerCounts': workerCounts,
    };
  } finally {
    await first.close(timeout: const Duration(seconds: 5)).catchError((_) {});
    await second.close(timeout: const Duration(seconds: 5)).catchError((_) {});
  }
}

Future<Map<String, Object?>> _rabbitReplicaScenario(
  String runId,
  int count,
) async {
  final topic = 'podbus.fault.rabbit.replicas.$runId';
  final exchange = 'podbus.fault.rabbit.replicas.exchange.$runId';
  final deadExchange = '$exchange.dead';
  final first = RabbitMqMessageBus(
    config: _rabbitConfig(
      url: _directRabbitUrl,
      exchange: exchange,
      deadExchange: deadExchange,
      connectionName: 'fault-rabbit-replica-one',
    ),
  );
  final second = RabbitMqMessageBus(
    config: _rabbitConfig(
      url: _directRabbitUrl,
      exchange: exchange,
      deadExchange: deadExchange,
      connectionName: 'fault-rabbit-replica-two',
    ),
  );
  final unique = <int>{};
  final duplicates = <int>[];
  final workerCounts = <String, int>{'one': 0, 'two': 0};
  final done = Completer<void>();

  Future<void> handle(String worker, Map<String, Object?> payload) async {
    await Future<void>.delayed(const Duration(milliseconds: 3));
    final id = payload['id']! as int;
    workerCounts[worker] = workerCounts[worker]! + 1;
    if (!unique.add(id)) duplicates.add(id);
    if (unique.length == count && !done.isCompleted) done.complete();
  }

  try {
    await first.connect();
    await second.connect();
    await first.worker<Map<String, Object?>>(
      topic,
      durableName: 'fault-rabbit-replicas-$runId',
      concurrency: 4,
      handler: (_, payload) => handle('one', payload),
    );
    await second.worker<Map<String, Object?>>(
      topic,
      durableName: 'fault-rabbit-replicas-$runId',
      concurrency: 4,
      handler: (_, payload) => handle('two', payload),
    );
    for (var id = 0; id < count; id += 1) {
      await first.enqueue(topic, {'id': id});
    }
    await done.future.timeout(_scenarioTimeout);
    if (workerCounts.values.where((value) => value > 0).length < 2) {
      throw StateError('RabbitMQ jobs were not consumed by both replicas.');
    }
    if (duplicates.isNotEmpty) {
      throw StateError(
        'RabbitMQ replica test observed duplicates: $duplicates',
      );
    }
    return {
      'published': count,
      'unique': unique.length,
      'duplicates': duplicates.length,
      'workerCounts': workerCounts,
    };
  } finally {
    await first.close(timeout: const Duration(seconds: 5)).catchError((_) {});
    await second.close(timeout: const Duration(seconds: 5)).catchError((_) {});
  }
}

Future<Map<String, Object?>> _natsStopBeforeConfirm(
  FaultEnvironment environment,
  String runId,
) async {
  final topic = 'podbus.fault.nats.confirm.$runId';
  final stream = 'PODBUS_FAULT_NATS_CONFIRM_$runId';
  final deliveries = <String>[];
  var factories = 0;
  final queue = ResilientDurableJobQueue(
    factory: () {
      factories += 1;
      return NatsJetStreamJobQueue(
        config: _natsConfig(url: _natsProxyUrl, stream: stream, topic: topic),
      );
    },
    policy: _faultReconnectPolicy(),
  );
  try {
    await queue.connect();
    await queue.worker<Map<String, Object?>>(
      topic,
      durableName: 'confirm-workers',
      handler: (_, payload) async => deliveries.add(payload['id']! as String),
    );
    await environment.toxiproxy.addLatency(
      proxy: _natsProxyName,
      toxic: 'delay-confirm',
      stream: 'downstream',
      latency: const Duration(seconds: 4),
    );
    final publish = queue.enqueue(topic, {
      'id': 'confirm-race',
    }, idempotencyKey: '$runId-confirm-race');
    await Future<void>.delayed(const Duration(milliseconds: 300));
    await environment.stopService('nats');
    await Future<void>.delayed(const Duration(seconds: 1));
    await environment.startService('nats');
    await environment.waitForService('nats');
    await environment.toxiproxy.removeToxic(
      proxy: _natsProxyName,
      toxic: 'delay-confirm',
    );
    await publish.timeout(_scenarioTimeout);
    await waitUntil(
      () => deliveries.contains('confirm-race'),
      timeout: _scenarioTimeout,
      description: 'NATS delivery after broker stop before confirmation',
    );
    final duplicates =
        deliveries.where((id) => id == 'confirm-race').length - 1;
    if (duplicates != 0) {
      throw StateError('NATS message-id deduplication failed: $deliveries');
    }
    return {
      'factoryCalls': factories,
      'deliveries': deliveries,
      'duplicates': duplicates,
    };
  } finally {
    await environment.toxiproxy
        .removeToxic(proxy: _natsProxyName, toxic: 'delay-confirm')
        .catchError((_) {});
    await queue.close(timeout: const Duration(seconds: 5)).catchError((_) {});
  }
}

Future<Map<String, Object?>> _rabbitStopBeforeConfirm(
  FaultEnvironment environment,
  String runId,
) async {
  final topic = 'podbus.fault.rabbit.confirm.$runId';
  final exchange = 'podbus.fault.rabbit.confirm.exchange.$runId';
  final deadExchange = '$exchange.dead';
  final deliveries = <String>[];
  var factories = 0;
  final queue = ResilientDurableJobQueue(
    factory: () {
      factories += 1;
      return RabbitMqMessageBus(
        config: _rabbitConfig(
          url: _rabbitProxyUrl,
          exchange: exchange,
          deadExchange: deadExchange,
          connectionName: 'fault-rabbit-confirm-$factories',
          confirmTimeout: const Duration(seconds: 1),
        ),
      );
    },
    policy: _faultReconnectPolicy(),
  );
  try {
    await queue.connect();
    await queue.worker<Map<String, Object?>>(
      topic,
      durableName: 'fault-rabbit-confirm-$runId',
      handler: (_, payload) async => deliveries.add(payload['id']! as String),
    );
    await environment.toxiproxy.addLatency(
      proxy: _rabbitProxyName,
      toxic: 'delay-confirm',
      stream: 'downstream',
      latency: const Duration(seconds: 4),
    );
    final publish = queue.enqueue(topic, {'id': 'confirm-race'});
    await Future<void>.delayed(const Duration(milliseconds: 300));
    await environment.stopService('rabbitmq');
    await Future<void>.delayed(const Duration(seconds: 1));
    await environment.startService('rabbitmq');
    await environment.waitForService('rabbitmq');
    await environment.toxiproxy.removeToxic(
      proxy: _rabbitProxyName,
      toxic: 'delay-confirm',
    );
    await publish.timeout(_scenarioTimeout);
    await waitUntil(
      () => deliveries.contains('confirm-race'),
      timeout: _scenarioTimeout,
      description: 'RabbitMQ delivery after broker stop before confirmation',
    );
    final duplicateCount =
        deliveries.where((id) => id == 'confirm-race').length - 1;
    return {
      'factoryCalls': factories,
      'deliveries': deliveries,
      'duplicatesAllowedByAtLeastOnce': math.max(0, duplicateCount),
    };
  } finally {
    await environment.toxiproxy
        .removeToxic(proxy: _rabbitProxyName, toxic: 'delay-confirm')
        .catchError((_) {});
    await queue.close(timeout: const Duration(seconds: 5)).catchError((_) {});
  }
}

Future<Map<String, Object?>> _natsShutdownDuringDlqAck(
  FaultEnvironment environment,
  String runId,
) async {
  final topic = 'podbus.fault.nats.shutdown.$runId';
  final stream = 'PODBUS_FAULT_NATS_SHUTDOWN_$runId';
  final durable = 'fault-nats-shutdown-$runId';
  final entered = Completer<void>();
  final release = Completer<void>();
  final first = NatsJetStreamJobQueue(
    config: _natsConfig(
      url: _natsProxyUrl,
      stream: stream,
      topic: topic,
      ackWait: const Duration(seconds: 1),
    ),
    messagingConfig: MessagingConfig(
      shutdownTimeout: const Duration(milliseconds: 750),
    ),
    fetchTimeout: const Duration(milliseconds: 100),
  );
  Object? closeError;
  try {
    await first.connect();
    await first.worker<Map<String, Object?>>(
      topic,
      durableName: durable,
      retryPolicy: RetryPolicy(
        maxAttempts: 1,
        initialDelay: Duration.zero,
        maxDelay: Duration.zero,
      ),
      deadLetterPolicy: DeadLetterPolicy(
        enabled: true,
        destination: '$topic.dead',
        includeOriginalPayload: true,
      ),
      handler: (_, _) async {
        if (!entered.isCompleted) entered.complete();
        await release.future;
        throw StateError('forced terminal failure');
      },
    );
    await first.enqueue(topic, {
      'id': 'shutdown-race',
    }, idempotencyKey: '$runId-nats-shutdown-race');
    await entered.future.timeout(_scenarioTimeout);
    await environment.toxiproxy.addLatency(
      proxy: _natsProxyName,
      toxic: 'delay-nats-dlq-ack',
      stream: 'downstream',
      latency: const Duration(seconds: 5),
    );
    release.complete();
    await Future<void>.delayed(const Duration(milliseconds: 200));
    try {
      await first.close(timeout: const Duration(milliseconds: 750));
    } on Object catch (error) {
      closeError = error;
    }
    await environment.toxiproxy.removeToxic(
      proxy: _natsProxyName,
      toxic: 'delay-nats-dlq-ack',
    );

    final second = NatsJetStreamJobQueue(
      config: _natsConfig(
        url: _natsProxyUrl,
        stream: stream,
        topic: topic,
        ackWait: const Duration(seconds: 1),
      ),
      fetchTimeout: const Duration(milliseconds: 100),
    );
    final redelivered = Completer<Map<String, Object?>>();
    try {
      await second.connect();
      await second.worker<Map<String, Object?>>(
        topic,
        durableName: durable,
        handler: (_, payload) async {
          if (!redelivered.isCompleted) redelivered.complete(payload);
        },
      );
      final payload = await redelivered.future.timeout(_scenarioTimeout);
      if (payload['id'] != 'shutdown-race') {
        throw StateError('Unexpected NATS redelivered payload: $payload');
      }
    } finally {
      await second
          .close(timeout: const Duration(seconds: 5))
          .catchError((_) {});
    }
    return {
      'closeRaised': closeError != null,
      'closeError': closeError?.toString(),
      'sourceRedelivered': true,
    };
  } finally {
    if (!release.isCompleted) release.complete();
    await environment.toxiproxy
        .removeToxic(proxy: _natsProxyName, toxic: 'delay-nats-dlq-ack')
        .catchError((_) {});
    await first.close(timeout: const Duration(seconds: 2)).catchError((_) {});
  }
}

Future<Map<String, Object?>> _rabbitShutdownDuringRetryConfirm(
  FaultEnvironment environment,
  String runId,
) async {
  final topic = 'podbus.fault.rabbit.retry-shutdown.$runId';
  final exchange = 'podbus.fault.rabbit.retry-shutdown.exchange.$runId';
  final deadExchange = '$exchange.dead';
  final durable = 'fault-rabbit-retry-shutdown-$runId';
  final entered = Completer<void>();
  final release = Completer<void>();
  final first = RabbitMqMessageBus(
    config: _rabbitConfig(
      url: _rabbitProxyUrl,
      exchange: exchange,
      deadExchange: deadExchange,
      connectionName: 'fault-rabbit-retry-shutdown-first',
      confirmTimeout: const Duration(seconds: 10),
    ),
    messagingConfig: MessagingConfig(
      shutdownTimeout: const Duration(milliseconds: 750),
    ),
  );
  Object? closeError;
  try {
    await first.connect();
    await first.worker<Map<String, Object?>>(
      topic,
      durableName: durable,
      retryPolicy: RetryPolicy(
        maxAttempts: 2,
        initialDelay: const Duration(seconds: 1),
        maxDelay: const Duration(seconds: 1),
        jitter: 0,
      ),
      handler: (_, _) async {
        if (!entered.isCompleted) entered.complete();
        await release.future;
        throw StateError('forced retryable failure');
      },
    );
    await first.enqueue(topic, {'id': 'retry-shutdown-race'});
    await entered.future.timeout(_scenarioTimeout);
    await environment.toxiproxy.addLatency(
      proxy: _rabbitProxyName,
      toxic: 'delay-retry-confirm',
      stream: 'downstream',
      latency: const Duration(seconds: 5),
    );
    release.complete();
    await Future<void>.delayed(const Duration(milliseconds: 200));
    try {
      await first.close(timeout: const Duration(milliseconds: 750));
    } on Object catch (error) {
      closeError = error;
    }
    await environment.toxiproxy.removeToxic(
      proxy: _rabbitProxyName,
      toxic: 'delay-retry-confirm',
    );

    final second = RabbitMqMessageBus(
      config: _rabbitConfig(
        url: _rabbitProxyUrl,
        exchange: exchange,
        deadExchange: deadExchange,
        connectionName: 'fault-rabbit-retry-shutdown-second',
      ),
    );
    final deliveries = <Map<String, Object?>>[];
    final redelivered = Completer<void>();
    try {
      await second.connect();
      await second.worker<Map<String, Object?>>(
        topic,
        durableName: durable,
        handler: (_, payload) async {
          deliveries.add(payload);
          if (!redelivered.isCompleted) redelivered.complete();
        },
      );
      await redelivered.future.timeout(_scenarioTimeout);
      if (deliveries.first['id'] != 'retry-shutdown-race') {
        throw StateError('Unexpected retry-race payload: ${deliveries.first}');
      }
      await Future<void>.delayed(const Duration(seconds: 2));
    } finally {
      await second
          .close(timeout: const Duration(seconds: 5))
          .catchError((_) {});
    }
    return {
      'closeRaised': closeError != null,
      'closeError': closeError?.toString(),
      'sourceRedelivered': true,
      'deliveriesObserved': deliveries.length,
      'duplicatesAllowedByAtLeastOnce': math.max(0, deliveries.length - 1),
    };
  } finally {
    if (!release.isCompleted) release.complete();
    await environment.toxiproxy
        .removeToxic(proxy: _rabbitProxyName, toxic: 'delay-retry-confirm')
        .catchError((_) {});
    await first.close(timeout: const Duration(seconds: 2)).catchError((_) {});
  }
}

Future<Map<String, Object?>> _rabbitShutdownDuringDlqConfirm(
  FaultEnvironment environment,
  String runId,
) async {
  final topic = 'podbus.fault.rabbit.shutdown.$runId';
  final exchange = 'podbus.fault.rabbit.shutdown.exchange.$runId';
  final deadExchange = '$exchange.dead';
  final durable = 'fault-rabbit-shutdown-$runId';
  final entered = Completer<void>();
  final release = Completer<void>();
  final first = RabbitMqMessageBus(
    config: _rabbitConfig(
      url: _rabbitProxyUrl,
      exchange: exchange,
      deadExchange: deadExchange,
      connectionName: 'fault-rabbit-shutdown-first',
      confirmTimeout: const Duration(seconds: 10),
    ),
    messagingConfig: MessagingConfig(
      shutdownTimeout: const Duration(milliseconds: 750),
    ),
  );
  Object? closeError;
  try {
    await first.connect();
    await first.worker<Map<String, Object?>>(
      topic,
      durableName: durable,
      retryPolicy: RetryPolicy(
        maxAttempts: 1,
        initialDelay: Duration.zero,
        maxDelay: Duration.zero,
      ),
      deadLetterPolicy: DeadLetterPolicy(
        enabled: true,
        destination: '$topic.dead',
        includeOriginalPayload: true,
      ),
      handler: (_, _) async {
        if (!entered.isCompleted) entered.complete();
        await release.future;
        throw StateError('forced failure');
      },
    );
    await first.enqueue(topic, {'id': 'shutdown-race'});
    await entered.future.timeout(_scenarioTimeout);
    await environment.toxiproxy.addLatency(
      proxy: _rabbitProxyName,
      toxic: 'delay-dlq-confirm',
      stream: 'downstream',
      latency: const Duration(seconds: 5),
    );
    release.complete();
    await Future<void>.delayed(const Duration(milliseconds: 200));
    try {
      await first.close(timeout: const Duration(milliseconds: 750));
    } on Object catch (error) {
      closeError = error;
    }
    await environment.toxiproxy.removeToxic(
      proxy: _rabbitProxyName,
      toxic: 'delay-dlq-confirm',
    );

    final second = RabbitMqMessageBus(
      config: _rabbitConfig(
        url: _rabbitProxyUrl,
        exchange: exchange,
        deadExchange: deadExchange,
        connectionName: 'fault-rabbit-shutdown-second',
      ),
    );
    final redelivered = Completer<Map<String, Object?>>();
    try {
      await second.connect();
      await second.worker<Map<String, Object?>>(
        topic,
        durableName: durable,
        handler: (_, payload) async {
          if (!redelivered.isCompleted) redelivered.complete(payload);
        },
      );
      final payload = await redelivered.future.timeout(_scenarioTimeout);
      if (payload['id'] != 'shutdown-race') {
        throw StateError('Unexpected redelivered payload: $payload');
      }
    } finally {
      await second
          .close(timeout: const Duration(seconds: 5))
          .catchError((_) {});
    }
    return {
      'closeRaised': closeError != null,
      'closeError': closeError?.toString(),
      'sourceRedelivered': true,
    };
  } finally {
    if (!release.isCompleted) release.complete();
    await environment.toxiproxy
        .removeToxic(proxy: _rabbitProxyName, toxic: 'delay-dlq-confirm')
        .catchError((_) {});
    await first.close(timeout: const Duration(seconds: 2)).catchError((_) {});
  }
}

Future<Map<String, Object?>> _slowConsumers(
  String runId, {
  required String profile,
}) async {
  final count = profile == 'full' ? 2000 : 300;
  final nats = await _natsSlowConsumer(runId, count);
  final rabbit = await _rabbitSlowConsumer(runId, count);
  return {'nats': nats, 'rabbitmq': rabbit};
}

Future<Map<String, Object?>> _natsSlowConsumer(String runId, int count) async {
  final topic = 'podbus.fault.nats.slow.$runId';
  final stream = 'PODBUS_FAULT_NATS_SLOW_$runId';
  const concurrency = 8;
  final queue = NatsJetStreamJobQueue(
    config: _natsConfig(url: _directNatsUrl, stream: stream, topic: topic),
    fetchBatchSize: 64,
    fetchTimeout: const Duration(milliseconds: 100),
  );
  return _runSlowConsumer(
    name: 'nats',
    count: count,
    concurrency: concurrency,
    connect: queue.connect,
    startWorker: (handler) => queue.worker<Map<String, Object?>>(
      topic,
      durableName: 'slow-workers',
      concurrency: concurrency,
      handler: handler,
    ),
    enqueue: (id) => queue.enqueue(topic, {
      'id': id,
    }, idempotencyKey: '$runId-slow-nats-$id'),
    close: () => queue.close(timeout: const Duration(seconds: 10)),
  );
}

Future<Map<String, Object?>> _rabbitSlowConsumer(
  String runId,
  int count,
) async {
  final topic = 'podbus.fault.rabbit.slow.$runId';
  final exchange = 'podbus.fault.rabbit.slow.exchange.$runId';
  const concurrency = 8;
  final queue = RabbitMqMessageBus(
    config: _rabbitConfig(
      url: _directRabbitUrl,
      exchange: exchange,
      deadExchange: '$exchange.dead',
      connectionName: 'fault-rabbit-slow',
    ),
  );
  return _runSlowConsumer(
    name: 'rabbitmq',
    count: count,
    concurrency: concurrency,
    connect: queue.connect,
    startWorker: (handler) => queue.worker<Map<String, Object?>>(
      topic,
      durableName: 'fault-rabbit-slow-$runId',
      concurrency: concurrency,
      handler: handler,
    ),
    enqueue: (id) => queue.enqueue(topic, {'id': id}),
    close: () => queue.close(timeout: const Duration(seconds: 10)),
  );
}

Future<Map<String, Object?>> _runSlowConsumer({
  required String name,
  required int count,
  required int concurrency,
  required Future<void> Function() connect,
  required Future<Worker> Function(JobHandler<Map<String, Object?>> handler)
  startWorker,
  required Future<void> Function(int id) enqueue,
  required Future<void> Function() close,
}) async {
  final unique = <int>{};
  final duplicates = <int>[];
  var active = 0;
  var maxActive = 0;
  final rssBefore = ProcessInfo.currentRss;
  var peakRss = rssBefore;
  final done = Completer<void>();
  try {
    await connect();
    await startWorker((_, payload) async {
      active += 1;
      maxActive = math.max(maxActive, active);
      peakRss = math.max(peakRss, ProcessInfo.currentRss);
      await Future<void>.delayed(const Duration(milliseconds: 15));
      final id = payload['id']! as int;
      if (!unique.add(id)) duplicates.add(id);
      active -= 1;
      if (unique.length == count && !done.isCompleted) done.complete();
    });
    for (var id = 0; id < count; id += 1) {
      await enqueue(id);
    }
    await done.future.timeout(const Duration(minutes: 2));
    if (maxActive > concurrency) {
      throw StateError(
        '$name exceeded concurrency: $maxActive > $concurrency.',
      );
    }
    if (duplicates.isNotEmpty) {
      throw StateError(
        '$name healthy slow-consumer run duplicated $duplicates',
      );
    }
    return {
      'messages': count,
      'unique': unique.length,
      'duplicates': duplicates.length,
      'configuredConcurrency': concurrency,
      'maxObservedConcurrency': maxActive,
      'rssBeforeBytes': rssBefore,
      'peakRssBytes': peakRss,
      'rssGrowthBytes': math.max(0, peakRss - rssBefore),
    };
  } finally {
    await close().catchError((_) {});
  }
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

  String value(String name, {required String fallback}) =>
      _values[name] ?? fallback;

  Set<String> csvValue(String name) {
    final value = _values[name];
    if (value == null || value.trim().isEmpty) return const {};
    return {
      for (final item in value.split(','))
        if (item.trim().isNotEmpty) item.trim(),
    };
  }

  bool boolValue(String name, {required bool fallback}) {
    final value = _values[name];
    if (value == null) return fallback;
    return switch (value.toLowerCase()) {
      'true' || '1' || 'yes' => true,
      'false' || '0' || 'no' => false,
      _ => throw FormatException('Invalid boolean for --$name: $value'),
    };
  }
}
