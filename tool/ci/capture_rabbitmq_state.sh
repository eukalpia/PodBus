#!/usr/bin/env bash
set -uo pipefail

compose_file="${PODBUS_COMPOSE_FILE:-docker-compose.integration.yaml}"
output_dir="${PODBUS_DIAGNOSTICS_DIR:-test-results}"
mkdir -p "$output_dir"

compose=(docker compose -f "$compose_file")

"${compose[@]}" ps -a > "$output_dir/compose-ps.txt" 2>&1 || true
"${compose[@]}" logs --no-color rabbitmq-init rabbitmq \
  > "$output_dir/rabbitmq-startup.log" 2>&1 || true

container_id="$("${compose[@]}" ps -aq rabbitmq 2>/dev/null || true)"
if [[ -n "$container_id" ]]; then
  docker inspect "$container_id" \
    > "$output_dir/rabbitmq-container-inspect.json" 2>&1 || true

  volume_name="$(docker inspect \
    --format '{{range .Mounts}}{{if eq .Destination "/var/lib/rabbitmq"}}{{.Name}}{{end}}{{end}}' \
    "$container_id" 2>/dev/null || true)"

  if [[ -n "$volume_name" ]]; then
    docker volume inspect "$volume_name" \
      > "$output_dir/rabbitmq-volume-inspect.json" 2>&1 || true
    docker run --rm \
      --user 0:0 \
      --entrypoint /bin/sh \
      -v "$volume_name:/state:ro" \
      rabbitmq:3.13.7-management \
      -ec 'id; echo "--- state directory"; ls -lan /state; echo "--- permissions"; stat -c "%u:%g %a %n" /state /state/.erlang.cookie 2>&1 || true' \
      > "$output_dir/rabbitmq-volume-state.txt" 2>&1 || true
  fi
fi
