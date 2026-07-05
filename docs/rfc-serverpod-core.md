# RFC: Pluggable message bus and durable job queue backends for Serverpod

## Summary

Serverpod could expose small transport-agnostic interfaces for message bus and durable job queue backends while keeping NATS, RabbitMQ, Kafka, and other brokers as external adapters.

## Goals

- No hard dependency on NATS, RabbitMQ, Kafka, or PodBus in Serverpod core.
- Compatibility with current Serverpod Redis and server events behavior.
- Typed Serverpod model serialization where possible.
- Lifecycle hooks for server startup and shutdown.
- Health checks for broker connectivity.
- Worker mode support for background processes.

## Proposed Interfaces

Serverpod core can define minimal abstractions similar to:

- `MessageBus`
- `DurableJobQueue`
- `Subscription`
- `Worker`
- `MessageHeaders`
- `HealthCheckResult`

Adapters would live outside Serverpod core and register during application startup.

## Serialization

Serverpod models should use existing generated serialization. Adapters should also allow JSON fallback for plain Dart services and tests.

## Lifecycle

Server startup should connect configured messaging backends. Shutdown should drain subscriptions and workers before closing broker connections.

## Compatibility

Existing Serverpod Redis/server event features should remain unchanged. A pluggable backend should be additive and opt-in.

## Open Questions

- Which parts belong in Serverpod core versus adapter packages?
- How should generated protocol serialization be exposed to external packages?
- Should worker mode be a first-class Serverpod runtime mode?
- What health check format best fits existing Serverpod observability?
