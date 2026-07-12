#!/usr/bin/env bash
set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "usage: $0 <service> [service ...]" >&2
  exit 64
fi

compose_file="${PODBUS_COMPOSE_FILE:-docker-compose.integration.yaml}"
output_dir="${PODBUS_DIAGNOSTICS_DIR:-test-results}"
mkdir -p "$output_dir"

compose=(docker compose -f "$compose_file")

# A clean project state is essential for RabbitMQ: an anonymous or stale volume
# can leave the Erlang cookie owned by root and make the broker fail with EACCES.
"${compose[@]}" down -v --remove-orphans \
  > "$output_dir/compose-down.log" 2>&1 || true
"${compose[@]}" config > "$output_dir/compose-config.yaml"

set +e
"${compose[@]}" up -d "$@" 2>&1 | tee "$output_dir/compose-up.log"
status=${PIPESTATUS[0]}
set -e

"${compose[@]}" ps -a > "$output_dir/compose-ps-start.txt" 2>&1 || true

for service in "$@"; do
  if [[ "$service" == "rabbitmq" || "$service" == "toxiproxy" ]]; then
    bash tool/ci/capture_rabbitmq_state.sh || true
    break
  fi
done

if [[ $status -ne 0 ]]; then
  "${compose[@]}" logs --no-color \
    > "$output_dir/compose-start-failure.log" 2>&1 || true
  exit "$status"
fi
