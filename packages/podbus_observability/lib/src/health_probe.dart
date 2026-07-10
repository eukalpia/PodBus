import 'dart:convert';

import 'package:podbus_core/podbus_core.dart';

typedef NamedHealthCheck = Future<HealthCheckResult> Function();

final class HealthProbeResponse {
  const HealthProbeResponse({
    required this.statusCode,
    required this.status,
    required this.components,
    required this.generatedAt,
  });

  final int statusCode;
  final HealthStatus status;
  final Map<String, HealthCheckResult> components;
  final DateTime generatedAt;

  String toJson() {
    return jsonEncode({
      'status': status.name,
      'generatedAt': generatedAt.toUtc().toIso8601String(),
      'components': {
        for (final entry in components.entries)
          entry.key: {
            'status': entry.value.status.name,
            if (entry.value.message != null) 'message': entry.value.message,
            'details': entry.value.details,
          },
      },
    });
  }
}

final class PodBusHealthProbe {
  PodBusHealthProbe({
    required Map<String, NamedHealthCheck> checks,
    DateTime Function()? clock,
    this.timeout = const Duration(seconds: 3),
  }) : checks = Map.unmodifiable(checks),
       _clock = clock ?? DateTime.now {
    if (checks.isEmpty) {
      throw const MessagingConfigurationException(
        'Health probe requires at least one component.',
      );
    }
    if (timeout <= Duration.zero) {
      throw const MessagingConfigurationException(
        'Health probe timeout must be greater than zero.',
      );
    }
  }

  final Map<String, NamedHealthCheck> checks;
  final DateTime Function() _clock;
  final Duration timeout;

  Future<HealthProbeResponse> readiness() async {
    final components = await _runChecks();
    final status = _aggregate(components.values);
    return HealthProbeResponse(
      statusCode: status == HealthStatus.healthy ? 200 : 503,
      status: status,
      components: components,
      generatedAt: _clock(),
    );
  }

  Future<HealthProbeResponse> liveness() async {
    final components = await _runChecks();
    final unhealthy = components.values.any(
      (result) => result.status == HealthStatus.unhealthy,
    );
    return HealthProbeResponse(
      statusCode: unhealthy ? 503 : 200,
      status: unhealthy ? HealthStatus.unhealthy : HealthStatus.healthy,
      components: components,
      generatedAt: _clock(),
    );
  }

  Future<Map<String, HealthCheckResult>> _runChecks() async {
    final results = <String, HealthCheckResult>{};
    await Future.wait([
      for (final entry in checks.entries)
        () async {
          try {
            results[entry.key] = await entry.value().timeout(timeout);
          } on Object catch (error) {
            results[entry.key] = HealthCheckResult.unhealthy(
              message: 'Health check failed.',
              details: {'errorType': error.runtimeType.toString()},
            );
          }
        }(),
    ]);
    return Map.unmodifiable(results);
  }

  static HealthStatus _aggregate(Iterable<HealthCheckResult> values) {
    if (values.any((result) => result.status == HealthStatus.unhealthy)) {
      return HealthStatus.unhealthy;
    }
    if (values.any((result) => result.status == HealthStatus.degraded)) {
      return HealthStatus.degraded;
    }
    return HealthStatus.healthy;
  }
}
