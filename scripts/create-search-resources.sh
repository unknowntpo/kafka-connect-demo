#!/usr/bin/env bash
set -euo pipefail

if curl -fsS -I "http://localhost:9200/product-events" >/dev/null 2>&1; then
  exit 0
fi

curl -fsS -X PUT "http://localhost:9200/product-events" \
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
