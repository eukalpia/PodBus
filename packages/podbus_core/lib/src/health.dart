enum HealthStatus { healthy, degraded, unhealthy }

final class HealthCheckResult {
  const HealthCheckResult({
    required this.status,
    required this.checkedAt,
    this.message,
    this.details = const {},
  });

  factory HealthCheckResult.healthy({
    String? message,
    Map<String, Object?> details = const {},
    DateTime? checkedAt,
  }) {
    return HealthCheckResult(
      status: HealthStatus.healthy,
      checkedAt: checkedAt ?? DateTime.now(),
      message: message,
      details: details,
    );
  }

  factory HealthCheckResult.degraded({
    String? message,
    Map<String, Object?> details = const {},
    DateTime? checkedAt,
  }) {
    return HealthCheckResult(
      status: HealthStatus.degraded,
      checkedAt: checkedAt ?? DateTime.now(),
      message: message,
      details: details,
    );
  }

  factory HealthCheckResult.unhealthy({
    String? message,
    Map<String, Object?> details = const {},
    DateTime? checkedAt,
  }) {
    return HealthCheckResult(
      status: HealthStatus.unhealthy,
      checkedAt: checkedAt ?? DateTime.now(),
      message: message,
      details: details,
    );
  }

  final HealthStatus status;
  final DateTime checkedAt;
  final String? message;
  final Map<String, Object?> details;
}
