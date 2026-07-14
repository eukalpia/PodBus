# podbus_kafka

Experimental Kafka adapter for PodBus using native `librdkafka` bindings.

It provides producers, consumer groups, offset commits, dead-letter handling, and PodBus envelope/codec integration.

## Status

The package version is `0.1.0-beta.1`, but the Kafka adapter itself remains **experimental**. Integration tests are mandatory; production semantics and native-platform coverage can still change.

## Requirements

Install `librdkafka` on the target system before running the adapter.

## Install

```bash
dart pub add podbus_kafka
dart pub add podbus_core
```

Read the known limits in the [PodBus repository](https://github.com/eukalpia/PodBus).
