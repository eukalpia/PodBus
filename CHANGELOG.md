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
- Isolate-based NATS Core and JetStream stress runners with exact unique-message and duplicate verification.
- A mandatory 3.25-million-message qualification matrix for NATS Core, JetStream, and RabbitMQ.
- Twelve isolated broker and network fault scenarios using clean services and Toxiproxy.
- A real one-hour NATS and RabbitMQ resilience-soak gate with retained evidence.
- A beta qualification report and corresponding static documentation page.

### Changed

- RabbitMQ configuration supports encrypted `amqps` connections, bounded reconnect settings, mandatory publishing, and broker-native retry topology.
- RabbitMQ publishing and consuming use separate channels.
- RabbitMQ publisher confirmations use a configurable lane pool with one outstanding confirmation per AMQP channel.
- NATS JetStream publishing uses a PodBus-owned concurrent PubAck reply inbox instead of the globally mutexed `dart_nats` request/reply path.
- NATS and JetStream stress publishers and consumers run in independent isolates so one event loop cannot starve all socket readers.
- Temporary RabbitMQ subscriptions use exclusive, auto-delete queues.
- Integration broker startup now uses explicit readiness probes and deterministic Kafka topic provisioning.
- GitHub Actions are pinned to immutable commits.
- Analyzer diagnostics, stress metadata, broker logs, resource snapshots, and qualification evidence are retained as workflow artifacts.

### Fixed

- Retry jitter is now applied.
- RabbitMQ idempotency claims are released when publishing fails.
- RabbitMQ unroutable mandatory publishes fail instead of being silently dropped.
- RabbitMQ high-concurrency publisher confirms no longer enter the unsafe `dart_amqp 0.3.1` multi-ack mutation path.
- A failed or nacked RabbitMQ publish no longer poisons later work on its publisher lane.
- RabbitMQ reconnects invalidate queued operations that belong to an obsolete connection epoch.
- Concurrent JetStream PubAck responses can complete out of order without being serialized by one global request mutex.
- JetStream close, drain, inbox failure, and connection replacement fail outstanding confirmations instead of leaving them unresolved.
- Late JetStream confirmations after a timeout cannot satisfy a later publish.
- Kafka malformed job envelopes can be dead-lettered instead of stopping before policy handling.
- Kafka dead-letter records are flushed before the source offset is committed.
- Dead-letter policies now honor `includeOriginalPayload`.
- Wire metadata now carries registered message type names.
