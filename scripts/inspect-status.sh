#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:${CONNECT_HOST_PORT:-18083}}"
CONNECTORS="${CONNECTORS:-elasticsearch-sink-product-events file-sink-quick-demo}"

for connector in $CONNECTORS; do
  echo "=== $connector ==="
  curl -fsS "$CONNECT_URL/connectors/$connector/status" | jq . || true
done
