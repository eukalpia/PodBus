# Kafka

`podbus_kafka` is experimental.

Kafka is an event log. It should not pretend to offer normal queue ack/nack semantics.

Planned mapping:

- producer publishes records to topics
- consumer group processes records
- successful handler commits offset
- failed handler does not commit, or publishes to a configured dead-letter topic
- retry strategy must be explicit and documented

Generic request/reply is intentionally unsupported.
