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

Broker-backed integration tests are planned; the current tests are unit tests.
