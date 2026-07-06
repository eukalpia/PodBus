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
docker compose -f docker-compose.integration.yaml up -d nats rabbitmq kafka
PODBUS_STRESS_MESSAGES=10000 dart run tool/stress_transports.dart
```

The stress runner prints the scenario parameters before the result table. The
main matrix controls are:

- `PODBUS_STRESS_MODES`: `fast,durable,worker,failure`
- `PODBUS_STRESS_TRANSPORTS`: `nats,jetstream,rabbitmq,kafka`
- `PODBUS_STRESS_PAYLOAD_SIZES`: payload sizes in bytes, for example
  `256,1024,10240`
- `PODBUS_STRESS_CONSUMERS`: consumer or worker counts, for example `1,4,16`
- `PODBUS_STRESS_PRODUCERS`: concurrent producer windows, for example
  `1,4,16`
- `PODBUS_STRESS_HANDLER_SLEEP_MS`: worker handler delay, for example `5,50`
- `PODBUS_STRESS_BROKER`: free-form label recorded in the output table

Example worker matrix:

```bash
PODBUS_STRESS_MODES=worker PODBUS_STRESS_TRANSPORTS=jetstream,rabbitmq,kafka PODBUS_STRESS_MESSAGES=10000 PODBUS_STRESS_PAYLOAD_SIZES=256,1024 PODBUS_STRESS_CONSUMERS=1,4 PODBUS_STRESS_PRODUCERS=4 PODBUS_STRESS_HANDLER_SLEEP_MS=0,5 dart run tool/stress_transports.dart
```

A focused 100K local smoke run:

```bash
PODBUS_STRESS_MESSAGES=100000 PODBUS_STRESS_MODES=fast,durable,worker PODBUS_STRESS_TRANSPORTS=nats,jetstream,rabbitmq,kafka PODBUS_STRESS_PAYLOAD_SIZES=256 PODBUS_STRESS_CONSUMERS=1 PODBUS_STRESS_PRODUCERS=16 PODBUS_STRESS_HANDLER_SLEEP_MS=0 dart run tool/stress_transports.dart
```

The stress runner is for regression and bottleneck discovery on a developer
machine. It records unsupported combinations as `skipped`; do not treat its
output as a production benchmark.
