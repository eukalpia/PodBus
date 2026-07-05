# Reliability

PodBus exposes delivery semantics directly.

## At-most-once

Live pub/sub transports can lose messages if no subscriber is online or a connection drops before delivery.

## At-least-once

Durable queues should be treated as at-least-once. Handlers must tolerate duplicate execution.

## Idempotency

Use `MessageHeaders.idempotencyKey` or enqueue `idempotencyKey` for side-effecting jobs. The in-memory store is only for tests and local workflows.

## Retries

`RetryPolicy` models bounded retries with exponential backoff and max delay. A transport adapter should use native retry features when they exist.

## Dead-letter

`DeadLetterPolicy` describes where exhausted messages go. Dead-letter payloads should avoid leaking sensitive error details unless explicitly configured.

## Exactly-once

PodBus does not claim exactly-once delivery.
