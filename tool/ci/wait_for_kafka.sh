#!/usr/bin/env bash
set -euo pipefail

compose=(docker compose -f docker-compose.integration.yaml)
kafka_topics=(
  /opt/kafka/bin/kafka-topics.sh
  --bootstrap-server localhost:9092
)

for attempt in $(seq 1 90); do
  if "${compose[@]}" exec -T kafka "${kafka_topics[@]}" --list >/dev/null 2>&1; then
    "${compose[@]}" exec -T kafka "${kafka_topics[@]}" \
      --create \
      --if-not-exists \
      --topic podbus.tests.kafka.events \
      --partitions 3 \
      --replication-factor 1 >/dev/null
    exit 0
  fi
  sleep 1
done

echo "Kafka did not become ready within 90 seconds." >&2
"${compose[@]}" logs --no-color kafka >&2 || true
exit 1
