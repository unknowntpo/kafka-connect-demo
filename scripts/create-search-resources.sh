#!/usr/bin/env bash
set -euo pipefail

FORCE_RECREATE_INDEX="${FORCE_RECREATE_INDEX:-0}"
ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-http://localhost:${ELASTICSEARCH_HOST_PORT:-19200}}"

if curl -fsS -I "$ELASTICSEARCH_URL/product-events" >/dev/null 2>&1; then
  if [[ "$FORCE_RECREATE_INDEX" == "1" ]]; then
    curl -fsS -X DELETE "$ELASTICSEARCH_URL/product-events" >/dev/null
  else
    exit 0
  fi
fi

curl -fsS -X PUT "$ELASTICSEARCH_URL/product-events" \
  -H "Content-Type: application/json" \
  -d '{
    "mappings": {
      "properties": {
        "occurred_at": { "type": "date" },
        "event_id": { "type": "keyword" },
        "event_type": { "type": "keyword" },
        "product_id": { "type": "keyword" },
        "product_name": { "type": "keyword" },
        "coupon_id": { "type": "keyword" },
        "coupon_name": { "type": "keyword" },
        "scenario": { "type": "keyword" },
        "phase": { "type": "keyword" },
        "service": { "type": "keyword" },
        "severity": { "type": "keyword" },
        "user_id": { "type": "keyword" },
        "session_id": { "type": "keyword" },
        "failure_reason": { "type": "keyword" },
        "pipeline": { "type": "keyword" },
        "metadata_region": { "type": "keyword" },
        "metadata_campaign": { "type": "keyword" },
        "metadata_ai_profile_version": { "type": "keyword" },
        "remaining_stock": { "type": "integer" },
        "remaining_coupons": { "type": "integer" },
        "inventory_before": { "type": "integer" },
        "inventory_after": { "type": "integer" },
        "discount_percent": { "type": "integer" },
        "price": { "type": "double" },
        "message": { "type": "text" }
      }
    }
  }' >/dev/null
