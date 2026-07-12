#!/usr/bin/env bash
set -euo pipefail

compose_file="${PODBUS_COMPOSE_FILE:-docker-compose.integration.yaml}"
compose=(docker compose -f "$compose_file")

for attempt in $(seq 1 120); do
  if "${compose[@]}" exec -T rabbitmq rabbitmq-diagnostics -q check_running \
      >/dev/null 2>&1 && \
    "${compose[@]}" exec -T rabbitmq rabbitmq-diagnostics -q check_port_connectivity \
      >/dev/null 2>&1; then
    echo "RabbitMQ AMQP listeners are ready after ${attempt}s."
    exit 0
  fi

  container_id="$("${compose[@]}" ps -aq rabbitmq 2>/dev/null || true)"
  if [[ -n "$container_id" ]]; then
    state="$(docker inspect --format '{{.State.Status}}' "$container_id" 2>/dev/null || true)"
    if [[ "$state" == "exited" || "$state" == "dead" ]]; then
      echo "RabbitMQ container entered state '$state' before becoming ready." >&2
      bash tool/ci/capture_rabbitmq_state.sh || true
      exit 1
    fi
  fi

  sleep 1
done

echo "RabbitMQ did not expose its AMQP listeners within 120 seconds." >&2
bash tool/ci/capture_rabbitmq_state.sh || true
exit 1
