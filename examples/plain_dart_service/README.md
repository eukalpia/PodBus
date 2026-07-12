# Plain Dart resilience service

This example proves that PodBus is not coupled to Serverpod or another application framework.
It runs a resilient JetStream worker, exposes a minimal readiness endpoint with `dart:io`,
processes a probe job, and performs a graceful shutdown.

```bash
docker compose -f docker-compose.integration.yaml up -d nats
bash tool/ci/wait_for_nats.sh

dart run examples/plain_dart_service/bin/main.dart
# In another terminal:
dart run examples/plain_dart_service/bin/probe.dart
```

The CI smoke starts the service as a separate operating-system process. The probe publishes
through a separate PodBus client, verifies the worker side effect over HTTP, requests shutdown,
and requires the service process to exit successfully.
