# Testing

Run local checks:

```bash
dart format .
dart analyze
dart test packages/podbus_core/test packages/podbus_nats/test packages/podbus_rabbitmq/test packages/podbus_kafka/test packages/podbus_serverpod/test
```

Integration tests should use local Docker services only. No external cloud services are required.

Current integration Docker Compose services:

- NATS
- RabbitMQ
- Kafka
- PostgreSQL for Serverpod examples

Broker-backed integration tests are available for NATS, NATS JetStream,
RabbitMQ, and Kafka when `PODBUS_RUN_INTEGRATION_TESTS=true` is set and the
Docker services are running.

```bash
docker compose -f docker-compose.integration.yaml up -d nats rabbitmq kafka
PODBUS_RUN_INTEGRATION_TESTS=true dart test packages/podbus_nats/test packages/podbus_rabbitmq/test packages/podbus_kafka/test
```

Local transport stress smoke tests can be run with:

```bash
PODBUS_STRESS_MESSAGES=10000 PODBUS_STRESS_CONCURRENCY=250 dart run tool/stress_transports.dart
```

The stress runner is for regression and bottleneck discovery on a developer
machine. Do not treat its output as a production benchmark.
