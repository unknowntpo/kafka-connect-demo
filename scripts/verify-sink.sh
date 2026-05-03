#!/usr/bin/env bash
set -euo pipefail

ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-http://localhost:${ELASTICSEARCH_HOST_PORT:-19200}}"

curl -fsS "$ELASTICSEARCH_URL/product-events/_search?pretty" \
  -H "Content-Type: application/json" \
  -d '{"size":10,"sort":[{"occurred_at":"asc"}],"query":{"match_all":{}}}'
