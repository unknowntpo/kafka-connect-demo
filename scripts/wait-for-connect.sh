#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

wait_for_http() {
  local url="$1"
  local name="$2"
  local attempt
  for attempt in $(seq 1 60); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      echo "$name is ready"
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for $name" >&2
  return 1
}

cd "$ROOT_DIR"

wait_for_http http://localhost:9200/_cluster/health "Elasticsearch"
wait_for_http http://localhost:8083/connectors "Kafka Connect REST"
wait_for_http http://localhost:8083/connector-plugins "Kafka Connect plugins endpoint"
