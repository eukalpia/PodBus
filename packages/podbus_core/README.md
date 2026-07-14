# podbus_core

Transport-agnostic contracts and in-memory implementations for PodBus.

`podbus_core` contains publish/subscribe, request/reply, durable-job, retry, dead-letter, codec, capability, idempotency, resilience, and health abstractions without binding your application to one broker.

## Status

`0.1.0-beta.1` is a public beta. API changes are still possible before `1.0.0`. Broker-backed processing is at-least-once; exactly-once external side effects are not claimed.

## Install

```bash
dart pub add podbus_core
```

## Start here

Use this package directly for contracts, policies, typed codecs, and in-memory tests. Add a transport package such as `podbus_nats` or `podbus_rabbitmq` for broker connectivity.

Full guides, reliability semantics, and examples are maintained in the [PodBus repository](https://github.com/eukalpia/PodBus).
