#!/usr/bin/env bash
set -euo pipefail

curl -fsS "http://localhost:9200/product-events/_search?pretty" \
  -H "Content-Type: application/json" \
  -d '{"size":10,"sort":[{"occurred_at":"asc"}],"query":{"match_all":{}}}'
