#!/usr/bin/env bash
set -euo pipefail

api="${PODBUS_TOXIPROXY_API:-http://127.0.0.1:8474}"
for attempt in $(seq 1 60); do
  if curl --fail --silent --show-error "$api/version" >/dev/null; then
    exit 0
  fi
  sleep 1
done

echo "Toxiproxy did not become ready at $api" >&2
exit 1
