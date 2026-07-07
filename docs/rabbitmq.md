# RabbitMQ

`podbus_rabbitmq` maps the PodBus `MessageBus` and `DurableJobQueue` contracts
onto AMQP exchanges, queues, bindings, manual acknowledgements, and publisher
confirmations.

The adapter is usable for local and integration testing, but it is still not a
complete production RabbitMQ backend. The remaining gaps are listed below.

## Implemented

- Topic exchange publishing with routing keys.
- Queue declaration and binding.
- Queue groups by using one shared queue name.
- Manual ack for consumed events and jobs.
- Manual nack with optional requeue.
- Prefetch configuration.
- Durable queues when `RabbitMqMessagingConfig.durable` is true.
- Persistent messages when durable mode is enabled.
- Manual dead-letter republish through `DeadLetterPolicy.destination`.
- Publisher confirms for outgoing publishes.
- Health check for connection state and the last fatal worker failure.

## Publisher Confirms

`DartRabbitMqAdapter` enables AMQP publisher confirm mode on connect. Every
publish gets an internal confirmation id and `publish()` does not complete until
RabbitMQ ACKs that message. A broker NACK fails the publish with
`MessagingConnectionException`.

`RabbitMqMessagingConfig.publisherConfirmTimeout` bounds how long a publish can
wait for confirmation. The default is 5 seconds.

Publisher confirms prove that the broker accepted the publish. They do not prove
that at least one consumer processed the message, and without mandatory-return
handling they do not prove that a topic routing key matched a queue binding.

## Retry And Dead Letter

When a job handler fails, the bus republishes either a retry message or a
dead-letter message before acknowledging the original delivery. If that republish
fails or times out, the original delivery is left unacked and the health check
records the failure.

Malformed event headers are rejected with `nack(requeue: false)`. Malformed job
headers are treated as poison messages: with a dead-letter policy they are
republished to the configured dead-letter route before the source delivery is
acked; without a dead-letter policy they are rejected without requeue.

Delayed retry is still implemented with client-side delay before republish. A
production RabbitMQ retry strategy should use TTL retry queues or the delayed
message plugin, depending on deployment constraints.

## Not Implemented Yet

- Request/reply.
- Mandatory-return handling for unroutable publishes.
- Broker-side dead-letter exchange setup and tests.
- Automatic reconnect and topology rebuild.
- Explicit TLS setup for `amqps://`.
- Malformed header quarantine.
- Server-named exclusive auto-delete queues for anonymous subscriptions.
- Collision-resistant queue naming for arbitrary subject strings.

## Local Integration Tests

RabbitMQ integration tests are Docker-backed and disabled by default:

```bash
docker compose -f docker-compose.integration.yaml up -d rabbitmq
PODBUS_RUN_INTEGRATION_TESTS=true dart test packages/podbus_rabbitmq/test
```

These tests currently cover publish/consume and manual dead-letter republish.
More failure-mode tests are still needed before claiming production readiness.
