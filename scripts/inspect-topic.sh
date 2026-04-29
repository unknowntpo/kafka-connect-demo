#!/usr/bin/env bash
set -euo pipefail

TOPIC="${1:-product.events}"
MAX_MESSAGES="${2:-10}"
TIMEOUT_MS="${3:-5000}"

docker compose exec -T broker kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic "$TOPIC" \
  --from-beginning \
  --timeout-ms "$TIMEOUT_MS" \
  --max-messages "$MAX_MESSAGES" 2>/dev/null || true
