#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONNECTOR_CONFIGS=("$@")

if [[ ${#CONNECTOR_CONFIGS[@]} -eq 0 ]]; then
  CONNECTOR_CONFIGS=(
    "$ROOT_DIR/connectors/elasticsearch-sink-product-events.json"
    "$ROOT_DIR/connectors/elasticsearch-sink-product-events-dlq.json"
  )
fi

put_connector() {
  local file="$1"
  local name
  local payload
  name="$(jq -r '.name' "$file")"
  payload="$(jq -c '.config' "$file")"
  curl -fsS -X PUT \
    -H "Content-Type: application/json" \
    --data "$payload" \
    "http://localhost:8083/connectors/$name/config" | jq .
}

for connector_config in "${CONNECTOR_CONFIGS[@]}"; do
  put_connector "$connector_config"
done
