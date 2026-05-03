#!/usr/bin/env bash
set -euo pipefail

for connector in elasticsearch-sink-product-events; do
  echo "=== $connector ==="
  curl -fsS "http://localhost:8083/connectors/$connector/status" | jq .
done
