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
- GitHub Actions CI, integration tests, release packaging, contribution, and security documentation.

### Fixed

- Retry jitter is now applied.
- RabbitMQ idempotency claims are released when publishing fails.
- Kafka malformed job envelopes can be dead-lettered instead of stopping before policy handling.
- Kafka dead-letter records are flushed before the source offset is committed.
- Dead-letter policies now honor `includeOriginalPayload`.
- Wire metadata now carries registered message type names.
