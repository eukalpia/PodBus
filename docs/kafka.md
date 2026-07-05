# Kafka

`podbus_kafka` is experimental.

Kafka is an event log. It should not pretend to offer normal queue ack/nack semantics.

Current mapping:

- producer publishes records to topics
- consumer group processes records
- successful handler commits offset
- failed handler does not commit, or publishes to a configured dead-letter topic
- retry strategy must be explicit and documented

Generic request/reply is intentionally unsupported.

## Native dependency

The adapter uses `librdkafka` through Dart FFI. Install `librdkafka` on the
machine running the Dart process.

On macOS with Homebrew:

```bash
brew install librdkafka
```

If the library is installed in a non-standard location, set:

```bash
export PODBUS_LIBRDKAFKA_PATH=/path/to/librdkafka.dylib
```

Linux deployments should install the platform package that provides
`librdkafka.so.1`, or set `PODBUS_LIBRDKAFKA_PATH` to the exact shared library
path.

## Failure semantics

Kafka has no transport-level negative acknowledgement. PodBus does not add fake
ack/nack behavior on top of Kafka.

For event subscriptions and workers:

- a successful handler commits the consumed record offset
- a failed handler without a dead-letter policy leaves the offset uncommitted
- a failed worker with a dead-letter policy publishes the original envelope to
  the dead-letter topic and then commits the source offset

Retries should be modeled with explicit retry topics, delayed processing outside
Kafka, or a separate workflow engine. Automatic retry delays are intentionally
unsupported in this adapter.
