#!/usr/bin/env bash
set -euo pipefail

FORCE_RECREATE_INDEX="${FORCE_RECREATE_INDEX:-0}"

ensure_absent_or_skip() {
  local index="$1"
  if curl -fsS -I "http://localhost:9200/$index" >/dev/null 2>&1; then
    if [[ "$FORCE_RECREATE_INDEX" == "1" ]]; then
      curl -fsS -X DELETE "http://localhost:9200/$index" >/dev/null
    else
      return 1
    fi
  fi
  return 0
}

if ensure_absent_or_skip "product-events"; then
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
          "metadata_region": { "type": "keyword" },
          "metadata_campaign": { "type": "keyword" },
          "metadata_ai_profile_version": { "type": "keyword" },
          "remaining_coupons": { "type": "integer" },
          "discount_percent": { "type": "integer" },
          "price": { "type": "double" },
          "message": { "type": "text" }
        }
      }
    }' >/dev/null
fi

if ensure_absent_or_skip "product-events-dlq"; then
  curl -fsS -X PUT "http://localhost:9200/product-events-dlq" \
    -H "Content-Type: application/json" \
    -d '{
      "mappings": {
        "properties": {
          "raw_record": { "type": "text" },
          "pipeline": { "type": "keyword" },
          "source_topic": { "type": "keyword" },
          "source_partition": { "type": "integer" },
          "source_offset": { "type": "long" },
          "dlq_timestamp": { "type": "date" }
        }
      }
    }' >/dev/null
fi
