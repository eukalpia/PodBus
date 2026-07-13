# Production deployment

This guide describes the minimum operating standard for a service that uses PodBus. It is intentionally stricter than the examples in the repository.

## Choose the transport deliberately

Use NATS Core for low-latency events and request/reply where losing an in-flight message during a broker or process failure is acceptable. Use JetStream or RabbitMQ when work must survive restarts. Kafka remains experimental in PodBus until partition assignment, rebalance, and delivery-report behavior is proven across the supported librdkafka matrix.

Check `capabilities` during startup and fail before serving traffic when a required capability is missing.

```dart
void requireDurableJobs(DurableJobQueue queue) {
  queue.capabilities.requireAll({
    MessagingCapability.durableJobs,
    MessagingCapability.deadLettering,
    MessagingCapability.gracefulShutdown,
  });
}
```

## Database consistency

A database write followed by a broker publish is not atomic. Use `podbus_postgres` when both operations represent one business action.

```dart
await pool.runTx((transaction) async {
  await OrderRepository.insert(transaction, order);
  await outbox.enqueue(
    transaction,
    'order.created',
    order.toJson(),
    key: order.id,
    headers: MessageHeaders(
      correlationId: requestId,
      causationId: commandId,
    ),
  );
});
```

Run `PostgresOutboxRelay.runOnce()` from a supervised loop. Give every replica a unique worker ID. The relay uses leases and `FOR UPDATE SKIP LOCKED`, so multiple replicas may process the same outbox table safely. Broker delivery remains at-least-once; consumers still need an inbox or another idempotency boundary.

## Configuration baseline

- Keep message payloads below 1 MiB unless the broker and every consumer have been tested with a larger bound.
- Keep headers below 16 KiB.
- Use a shared PostgreSQL idempotency store for multiple replicas.
- Exclude original payloads and stack traces from dead letters by default.
- Set explicit request, publish-confirm, and shutdown timeouts.
- Give every durable consumer a stable, versioned name.
- Use a distinct broker identity per service with the minimum subject, topic, exchange, and queue permissions.
- Keep application publish concurrency bounded even when the transport supports concurrent confirmations.

## Transport security

Production broker endpoints must use encrypted connections:

- NATS: TLS plus a scoped token or username/password credentials exposed by the current adapter.
- RabbitMQ: `amqps://`, trusted certificate authorities, dedicated users, and separate virtual hosts where practical.
- Kafka: TLS plus SASL/SCRAM or the authentication mechanism required by the cluster.
- PostgreSQL: TLS with certificate verification; never use the integration-test credentials.

NATS permissions must include only the application subjects the service needs. A service using request/reply or JetStream PubAck also needs its own `_INBOX` reply subjects. Do not grant unrelated publishers broad access to `_INBOX.>`: PodBus uses cryptographically random NATS NUIDs for PubAck inboxes, but broker ACLs remain the primary authorization boundary.

For RabbitMQ, grant configure/write/read permissions only for the exchanges and queues owned by the service. Publisher-confirm lanes increase channel count, not the required routing permissions. Monitor channel totals and keep the configured lane count within broker policy.

Do not implement `onBadCertificate` by returning `true` in production. That converts TLS into expensive plaintext.

## Graceful shutdown

On `SIGTERM`:

1. stop accepting new HTTP or RPC traffic;
2. mark readiness unhealthy;
3. stop fetching new broker messages;
4. wait for active handlers and outbox publishes;
5. acknowledge or requeue messages according to handler outcome;
6. close broker connections;
7. exit before the platform termination deadline.

Set the container termination grace period longer than the PodBus shutdown timeout. The Kubernetes example uses 45 seconds and a 30-second application drain budget. JetStream drain is bounded by each outstanding publish-confirmation deadline; a lost PubAck must fail shutdown rather than pin the process indefinitely.

## Observability

`podbus_observability` provides:

- W3C `traceparent` and `tracestate` propagation;
- producer, consumer, request, and job spans through decorators;
- bounded-cardinality Prometheus output;
- structured JSON logs with redaction;
- readiness and liveness aggregation.

Do not place message IDs, customer IDs, email addresses, or arbitrary routing keys in Prometheus labels. High-cardinality labels eventually become a monitoring outage wearing a metrics badge.

## Capacity and retention

Size the broker for peak ingress, handler latency, retry bursts, and the period during which consumers may be unavailable. Configure:

- stream or queue retention;
- disk and memory alarms;
- maximum payload size;
- dead-letter retention;
- maximum delivery count;
- consumer pending limits;
- replication appropriate for the failure domain.

A dead-letter queue is not a backup. Export or retain business-critical failed work according to the application's recovery objectives.

## Release gate

A production release requires:

- the `Production gate` check to pass;
- the security and compatibility workflows to pass;
- a successful large-transport qualification run;
- all broker fault scenarios to pass without process leaks;
- a real one-hour resilience soak to complete with no missing acknowledged messages;
- a reviewed migration plan for wire-schema or database changes;
- dashboards and alerts installed before traffic is enabled;
- a rollback version that has already been exercised.

See [Beta qualification](beta-qualification.md), [Runbook](runbook.md), [Disaster recovery](disaster-recovery.md), and [Upgrading](upgrading.md).
