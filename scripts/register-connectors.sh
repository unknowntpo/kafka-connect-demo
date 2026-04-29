#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONNECTOR_CONFIG="${1:-$ROOT_DIR/connectors/elasticsearch-sink-product-events.json}"

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

put_connector "$CONNECTOR_CONFIG"
