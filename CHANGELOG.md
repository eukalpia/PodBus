# Changelog

All notable changes to PodBus are documented here. The project follows Semantic Versioning after the first stable release.

## Unreleased

### Added

- Cross-transport capability discovery.
- Typed codec registry with stable message types and schema versions.
- Configurable payload, header, and error-detail limits.
- Failure classification, structured metrics, duration metrics, and degraded health states.
- Bounded NATS Core and RabbitMQ subscription concurrency.
- Failure-atomic startup and best-effort graceful shutdown across transports and Serverpod.
- PostgreSQL transactional outbox with leased multi-replica relay processing.
- PostgreSQL inbox leases and persistent idempotency storage.
- W3C trace-context propagation and producer, consumer, request, and job instrumentation.
- Bounded-cardinality Prometheus metrics and redacted structured JSON logs.
- Framework-neutral readiness and liveness aggregation.
- Separate broker integration jobs, diagnostics artifacts, compatibility checks, and a single production gate.
- Nightly repeated integration, broker restart, and transport stress workflows.
- Release checksums, SPDX SBOM generation, and build provenance attestations.
- Production deployment, incident response, disaster recovery, upgrade, Kubernetes, and alerting examples.
- GitHub Actions CI, release packaging, contribution, and security documentation.

### Changed

- RabbitMQ configuration supports encrypted `amqps` connections, bounded reconnect settings, mandatory publishing, and broker-native retry topology.
- RabbitMQ publishing and consuming use separate channels.
- Temporary RabbitMQ subscriptions use exclusive, auto-delete queues.
- Integration broker startup now uses explicit readiness probes and deterministic Kafka topic provisioning.
- GitHub Actions are pinned to immutable commits.

### Fixed

- Retry jitter is now applied.
- RabbitMQ idempotency claims are released when publishing fails.
- RabbitMQ unroutable mandatory publishes fail instead of being silently dropped.
- Kafka malformed job envelopes can be dead-lettered instead of stopping before policy handling.
- Kafka dead-letter records are flushed before the source offset is committed.
- Dead-letter policies now honor `includeOriginalPayload`.
- Wire metadata now carries registered message type names.
