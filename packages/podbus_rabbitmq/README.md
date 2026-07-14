# podbus_rabbitmq

RabbitMQ messaging and durable-worker support for PodBus.

The adapter provides publisher-confirm lanes, mandatory routing, queue workers, broker-native retry topology, dead-letter queues, bounded concurrency, reconnect handling, and graceful shutdown.

## Status

`0.1.0-beta.1` is a public beta for controlled production evaluation. Delivery is at-least-once; handlers and external side effects must be idempotent.

## Install

```bash
dart pub add podbus_rabbitmq
dart pub add podbus_core
```

See the [publisher-lane operations guide](https://github.com/eukalpia/PodBus/blob/main/docs/rabbitmq-publisher-lanes.md) and the main [PodBus documentation](https://github.com/eukalpia/PodBus).
