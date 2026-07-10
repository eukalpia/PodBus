#!/usr/bin/env bash
set -euo pipefail

for attempt in $(seq 1 90); do
  if docker compose -f docker-compose.integration.yaml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    exit 0
  fi
  sleep 1
done

echo "Kafka did not become ready within 90 seconds." >&2
docker compose -f docker-compose.integration.yaml logs --no-color kafka >&2 || true
exit 1
