# Contributing to PodBus

Thank you for helping improve PodBus.

## Before opening a change

- Search existing issues and pull requests.
- Keep transport-specific behavior inside its adapter.
- Do not claim a delivery guarantee that the broker or implementation cannot prove.
- Add or update tests for every behavior change.
- Update the capability matrix and transport documentation when support changes.

## Local checks

```bash
dart pub get
dart format --output=none --set-exit-if-changed .
dart analyze .
dart test packages/podbus_core/test packages/podbus_nats/test packages/podbus_rabbitmq/test packages/podbus_kafka/test packages/podbus_serverpod/test
```

For broker-backed changes, also run the Docker integration suite documented in `docs/testing.md`.

## Pull requests

Keep each pull request focused. Explain:

- the failure mode or use case;
- the delivery semantics before and after the change;
- how the change was tested;
- compatibility or migration concerns.

Use conventional, imperative commit subjects such as `Fix Kafka DLQ commit ordering`.

## Code style

The repository uses the Dart formatter and the lints configured in `analysis_options.yaml`. Public APIs require documentation when their purpose or guarantees are not obvious from the name.
