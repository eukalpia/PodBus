import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math' as math;

import 'package:podbus_core/podbus_core.dart';
import 'package:podbus_nats/podbus_nats.dart';
import 'package:podbus_rabbitmq/podbus_rabbitmq.dart';

import 'src/fault_harness.dart';

Future<void> main(List<String> arguments) async {
  final options = _Options(arguments);
  final duration = parseDuration(options.value('duration', fallback: '1h'));
  final allowShort = options.boolValue('allow-short', fallback: false);
  if (!allowShort && duration < const Duration(hours: 1)) {
    throw ArgumentError(
      'Production soak duration must be at least one hour. '
      'Use --allow-short=true only for CI smoke validation.',
    );
  }
  if (duration > const Duration(hours: 24)) {
    throw ArgumentError('Maximum supported soak duration is 24 hours.');
  }
  final faultInterval = parseDuration(
    options.value('fault-interval', fallback: allowShort ? '5s' : '2m'),
  );
  final outage = parseDuration(
    options.value('outage', fallback: allowShort ? '1s' : '10s'),
  );
  final publishInterval = parseDuration(
    options.value('publish-interval', fallback: allowShort ? '20ms' : '100ms'),
  );
  final maxOperationErrors = options.intValue(
    'max-operation-errors',
    fallback: 0,
  );
  final maxRssGrowthBytes =
      options.intValue('max-rss-growth-mb', fallback: 512) * 1024 * 1024;
  final maxRecoveryP95Ms = options.intValue(
    'max-recovery-p95-ms',
    fallback: 30000,
  );
  if (maxOperationErrors < 0 || maxRssGrowthBytes < 0 || maxRecoveryP95Ms < 0) {
    throw ArgumentError('Soak thresholds must not be negative.');
  }
  final report = File(
    options.value('report', fallback: 'test-results/soak-summary.json'),
  );
  final environment = FaultEnvironment(
    composeFile: options.value(
      'compose-file',
      fallback: 'docker-compose.integration.yaml',
    ),
  );
  final runId = DateTime.now().microsecondsSinceEpoch.toString();
  final topicNats = 'podbus.soak.nats.$runId';
  final stream = 'PODBUS_SOAK_NATS_$runId';
  final topicRabbit = 'podbus.soak.rabbit.$runId';
  final exchange = 'podbus.soak.rabbit.exchange.$runId';

  await environment.waitForService('nats');
  await environment.waitForService('rabbitmq');
  await environment.waitForService('toxiproxy');
  await _configureProxies(environment);

  final natsStats = _DeliveryStats('nats');
  final rabbitStats = _DeliveryStats('rabbitmq');
  var natsFactories = 0;
  var rabbitFactories = 0;
  final reconnectLatenciesMs = <int>[];
  final faults = <Map<String, Object?>>[];
  final errors = <Map<String, Object?>>[];
  final rssStart = ProcessInfo.currentRss;
  var peakRss = rssStart;
  var stopFaults = false;

  final nats = ResilientDurableJobQueue(
    factory: () {
      natsFactories += 1;
      return NatsJetStreamJobQueue(
        config: NatsMessagingConfig(
          servers: [Uri.parse('nats://127.0.0.1:14222')],
          connectTimeout: const Duration(seconds: 1),
          requestTimeout: const Duration(seconds: 2),
          jetStream: NatsJetStreamConfig(
            enabled: true,
            streamName: stream,
            subjects: [topicNats],
            storage: NatsJetStreamStorage.file,
            maxAge: const Duration(hours: 30),
            consumerConfig: const NatsJetStreamConsumerConfig(
              ackWait: Duration(seconds: 5),
              maxDeliver: 100,
              maxAckPending: 4096,
            ),
          ),
        ),
        fetchTimeout: const Duration(milliseconds: 200),
        fetchBatchSize: 64,
      );
    },
    policy: _soakReconnectPolicy(),
  );

  final rabbit = ResilientDurableJobQueue(
    factory: () {
      rabbitFactories += 1;
      return RabbitMqMessageBus(
        config: RabbitMqMessagingConfig(
          uri: Uri.parse('amqp://guest:guest@127.0.0.1:15670'),
          exchange: exchange,
          deadLetterExchange: '$exchange.dead',
          connectTimeout: const Duration(seconds: 1),
          publisherConfirmTimeout: const Duration(seconds: 2),
          maxConnectionAttempts: 1,
          reconnectWaitTime: const Duration(milliseconds: 100),
          connectionName: 'podbus-soak-rabbit-$rabbitFactories',
          prefetchCount: 512,
        ),
      );
    },
    policy: _soakReconnectPolicy(),
  );

  final startedAt = DateTime.now().toUtc();
  final deadline = startedAt.add(duration);
  Future<void>? faultTask;
  try {
    await nats.connect();
    await rabbit.connect();
    await nats.worker<Map<String, Object?>>(
      topicNats,
      durableName: 'podbus-soak-nats',
      concurrency: 16,
      handler: (context, payload) async {
        natsStats.record(payload['id']! as int, context.attempt);
      },
    );
    await rabbit.worker<Map<String, Object?>>(
      topicRabbit,
      durableName: 'podbus-soak-rabbit-$runId',
      concurrency: 16,
      handler: (context, payload) async {
        rabbitStats.record(payload['id']! as int, context.attempt);
      },
    );

    faultTask = () async {
      var index = 0;
      while (!stopFaults) {
        await Future<void>.delayed(faultInterval);
        if (stopFaults) break;
        index += 1;
        final started = DateTime.now().toUtc();
        final target = index.isOdd ? 'nats' : 'rabbitmq';
        final kind = index % 4 == 0 ? 'broker-restart' : 'tcp-partition';
        try {
          if (kind == 'broker-restart') {
            await environment.restartService(target);
            await environment.waitForService(target);
          } else {
            final proxy = target == 'nats' ? _natsProxyName : _rabbitProxyName;
            await environment.toxiproxy.setEnabled(proxy, false);
            await Future<void>.delayed(outage);
            await environment.toxiproxy.setEnabled(proxy, true);
          }
          final finished = DateTime.now().toUtc();
          faults.add({
            'index': index,
            'target': target,
            'kind': kind,
            'startedAt': started.toIso8601String(),
            'finishedAt': finished.toIso8601String(),
            'durationMs': finished.difference(started).inMilliseconds,
          });
        } on Object catch (error, stackTrace) {
          errors.add({
            'stage': 'fault-injection',
            'target': target,
            'kind': kind,
            'error': error.toString(),
            'stackTrace': stackTrace.toString(),
          });
          await _restore(environment);
        }
      }
    }();

    var id = 0;
    while (DateTime.now().toUtc().isBefore(deadline)) {
      id += 1;
      final natsStarted = DateTime.now();
      try {
        await nats.enqueue(topicNats, {
          'id': id,
        }, idempotencyKey: '$runId-nats-$id');
        natsStats.accepted.add(id);
        final latency = DateTime.now().difference(natsStarted).inMilliseconds;
        if (latency >= 500) reconnectLatenciesMs.add(latency);
      } on Object catch (error, stackTrace) {
        errors.add({
          'stage': 'nats-enqueue',
          'id': id,
          'error': error.toString(),
          'stackTrace': stackTrace.toString(),
        });
      }

      final rabbitStarted = DateTime.now();
      try {
        await rabbit.enqueue(topicRabbit, {'id': id});
        rabbitStats.accepted.add(id);
        final latency = DateTime.now().difference(rabbitStarted).inMilliseconds;
        if (latency >= 500) reconnectLatenciesMs.add(latency);
      } on Object catch (error, stackTrace) {
        errors.add({
          'stage': 'rabbit-enqueue',
          'id': id,
          'error': error.toString(),
          'stackTrace': stackTrace.toString(),
        });
      }

      peakRss = math.max(peakRss, ProcessInfo.currentRss);
      await Future<void>.delayed(publishInterval);
    }

    stopFaults = true;
    await faultTask;
    await _restore(environment);
    await waitUntil(
      () =>
          natsStats.delivered.containsAll(natsStats.accepted) &&
          rabbitStats.delivered.containsAll(rabbitStats.accepted),
      timeout: allowShort
          ? const Duration(minutes: 2)
          : const Duration(minutes: 10),
      description: 'all acknowledged soak messages to drain',
      pollInterval: const Duration(milliseconds: 250),
    );
  } finally {
    stopFaults = true;
    await faultTask?.catchError((_) {});
    await _restore(environment);
    await nats.close(timeout: const Duration(seconds: 15)).catchError((_) {});
    await rabbit.close(timeout: const Duration(seconds: 15)).catchError((_) {});
  }

  final finishedAt = DateTime.now().toUtc();
  final natsMissing = natsStats.accepted.difference(natsStats.delivered);
  final rabbitMissing = rabbitStats.accepted.difference(rabbitStats.delivered);
  final rssGrowthBytes = math.max(0, peakRss - rssStart);
  final recoveryP95Ms = _percentile(reconnectLatenciesMs, 0.95);
  final failureReasons = <String>[
    if (natsMissing.isNotEmpty)
      'NATS lost ${natsMissing.length} acknowledged messages',
    if (rabbitMissing.isNotEmpty)
      'RabbitMQ lost ${rabbitMissing.length} acknowledged messages',
    if (errors.length > maxOperationErrors)
      'operation errors ${errors.length} exceeded $maxOperationErrors',
    if (rssGrowthBytes > maxRssGrowthBytes)
      'RSS growth $rssGrowthBytes exceeded $maxRssGrowthBytes bytes',
    if (recoveryP95Ms > maxRecoveryP95Ms)
      'recovery p95 ${recoveryP95Ms}ms exceeded ${maxRecoveryP95Ms}ms',
  ];
  final summary = {
    'runId': runId,
    'startedAt': startedAt.toIso8601String(),
    'finishedAt': finishedAt.toIso8601String(),
    'requestedDurationMs': duration.inMilliseconds,
    'actualDurationMs': finishedAt.difference(startedAt).inMilliseconds,
    'faultIntervalMs': faultInterval.inMilliseconds,
    'outageMs': outage.inMilliseconds,
    'faults': faults,
    'faultCount': faults.length,
    'factoryCalls': {'nats': natsFactories, 'rabbitmq': rabbitFactories},
    'nats': natsStats.toJson(missing: natsMissing),
    'rabbitmq': rabbitStats.toJson(missing: rabbitMissing),
    'recoveryLatenciesMs': reconnectLatenciesMs,
    'recoveryLatencyP50Ms': _percentile(reconnectLatenciesMs, 0.50),
    'recoveryLatencyP95Ms': recoveryP95Ms,
    'recoveryLatencyMaxMs': reconnectLatenciesMs.isEmpty
        ? 0
        : reconnectLatenciesMs.reduce(math.max),
    'errors': errors,
    'rssStartBytes': rssStart,
    'peakRssBytes': peakRss,
    'rssGrowthBytes': rssGrowthBytes,
    'thresholds': {
      'maxOperationErrors': maxOperationErrors,
      'maxRssGrowthBytes': maxRssGrowthBytes,
      'maxRecoveryP95Ms': maxRecoveryP95Ms,
    },
    'failureReasons': failureReasons,
    'success': failureReasons.isEmpty,
  };
  await report.parent.create(recursive: true);
  await report.writeAsString(
    const JsonEncoder.withIndent('  ').convert(summary),
    flush: true,
  );
  stdout.writeln(const JsonEncoder.withIndent('  ').convert(summary));

  if (failureReasons.isNotEmpty) {
    throw StateError('Soak failed: ${failureReasons.join('; ')}');
  }
}

const _natsProxyName = 'podbus-nats';
const _rabbitProxyName = 'podbus-rabbitmq';

ReconnectPolicy _soakReconnectPolicy() => const ReconnectPolicy(
  maxAttempts: 120,
  initialDelay: Duration(milliseconds: 100),
  maxDelay: Duration(seconds: 2),
  jitter: 0.2,
  recoveryTimeout: Duration(minutes: 3),
  healthCheckInterval: Duration(milliseconds: 500),
  healthCheckTimeout: Duration(seconds: 1),
);

Future<void> _configureProxies(FaultEnvironment environment) async {
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

Future<void> _restore(FaultEnvironment environment) async {
  for (final service in ['nats', 'rabbitmq']) {
    await environment.startService(service).catchError((_) {});
    await environment.waitForService(service).catchError((_) {});
  }
  await environment.toxiproxy
      .setEnabled(_natsProxyName, true)
      .catchError((_) {});
  await environment.toxiproxy
      .setEnabled(_rabbitProxyName, true)
      .catchError((_) {});
  await environment.toxiproxy.reset().catchError((_) {});
}

int _percentile(List<int> values, double percentile) {
  if (values.isEmpty) return 0;
  final sorted = [...values]..sort();
  final index = ((sorted.length - 1) * percentile).round();
  return sorted[index];
}

final class _DeliveryStats {
  _DeliveryStats(this.transport);

  final String transport;
  final Set<int> accepted = {};
  final Set<int> delivered = {};
  var deliveries = 0;
  var duplicates = 0;
  var redeliveries = 0;
  var maxAttempt = 1;

  void record(int id, int attempt) {
    deliveries += 1;
    if (!delivered.add(id)) duplicates += 1;
    if (attempt > 1) redeliveries += 1;
    maxAttempt = math.max(maxAttempt, attempt);
  }

  Map<String, Object?> toJson({required Set<int> missing}) => {
    'transport': transport,
    'accepted': accepted.length,
    'deliveries': deliveries,
    'uniqueDelivered': delivered.length,
    'duplicates': duplicates,
    'redeliveries': redeliveries,
    'maxAttempt': maxAttempt,
    'missing': missing.length,
    'missingSample': missing.take(50).toList(),
  };
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

  int intValue(String name, {required int fallback}) {
    final value = _values[name];
    return value == null ? fallback : int.parse(value);
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
