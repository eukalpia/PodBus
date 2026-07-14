# podbus_postgres

PostgreSQL reliability primitives for PodBus.

This package provides a transactional outbox, leased multi-replica relay processing, inbox leases, and persistent idempotency storage. It is intended for workflows where a database mutation and later broker publication must share a durable boundary.

## Status

`0.1.0-beta.1` is a public beta. Schema migrations and operational behavior must be reviewed before production rollout.

## Install

```bash
dart pub add podbus_postgres
dart pub add podbus_core
```

See the [reliability guide](https://github.com/eukalpia/PodBus/blob/main/docs/reliability.md) for delivery and failure semantics.
