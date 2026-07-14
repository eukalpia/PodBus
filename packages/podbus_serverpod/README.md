# podbus_serverpod

Optional Serverpod lifecycle integration for PodBus.

The helpers create a fresh Serverpod session for each message, close it after handling, coordinate startup and shutdown, and keep the underlying messaging contracts framework-neutral.

## Status

`0.1.0-beta.1` is a public beta. Verify session factories, shutdown deadlines, and idempotency behavior against your Serverpod application before production rollout.

## Install

```bash
dart pub add podbus_serverpod
dart pub add podbus_core
```

See the [Serverpod section](https://github.com/eukalpia/PodBus#serverpod) in the main documentation.
