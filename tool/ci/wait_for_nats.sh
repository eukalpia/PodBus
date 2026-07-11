#!/usr/bin/env bash
set -euo pipefail

for attempt in $(seq 1 60); do
  if curl --fail --silent --show-error http://127.0.0.1:8222/healthz >/dev/null; then
    exit 0
  fi
  sleep 1
done

echo "NATS did not become ready within 60 seconds." >&2
docker compose -f docker-compose.integration.yaml logs --no-color nats >&2 || true
exit 1
