#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONNECT_URL="${CONNECT_URL:-http://localhost:${CONNECT_HOST_PORT:-18083}}"
ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-http://localhost:${ELASTICSEARCH_HOST_PORT:-19200}}"
WAIT_FOR_ELASTICSEARCH="${WAIT_FOR_ELASTICSEARCH:-1}"

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

if [[ "$WAIT_FOR_ELASTICSEARCH" == "1" ]]; then
  wait_for_http "$ELASTICSEARCH_URL/_cluster/health" "Elasticsearch"
fi
wait_for_http "$CONNECT_URL/connectors" "Kafka Connect REST"
wait_for_http "$CONNECT_URL/connector-plugins" "Kafka Connect plugins endpoint"
