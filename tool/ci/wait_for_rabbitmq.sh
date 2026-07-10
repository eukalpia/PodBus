#!/usr/bin/env bash
set -euo pipefail

for attempt in $(seq 1 60); do
  if docker compose -f docker-compose.integration.yaml exec -T rabbitmq rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
    exit 0
  fi
  sleep 1
done

echo "RabbitMQ did not become ready within 60 seconds." >&2
docker compose -f docker-compose.integration.yaml logs --no-color rabbitmq >&2 || true
exit 1
