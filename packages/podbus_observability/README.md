# podbus_observability

Framework-neutral observability for PodBus.

The package includes W3C trace-context propagation, producer and consumer spans, bounded-cardinality Prometheus metrics, redacted JSON logs, and readiness/liveness aggregation.

## Status

`0.1.0-beta.1` is a public beta. Metric label allow-lists and cardinality limits should be reviewed for each deployment.

## Install

```bash
dart pub add podbus_observability
dart pub add podbus_core
```

Operational guidance is available in the [PodBus documentation](https://github.com/eukalpia/PodBus/tree/main/docs).
