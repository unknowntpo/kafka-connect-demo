#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KIBANA_URL="${KIBANA_URL:-http://localhost:5601}"
DASHBOARD_ID="hot-product-sales-dashboard"
DASHBOARD_DEFINITION="${DASHBOARD_DEFINITION:-$ROOT_DIR/dashboards/hot-product-sales-observability.ndjson}"
DASHBOARD_TIME_FROM="${DASHBOARD_TIME_FROM:-now-3h}"
DASHBOARD_TIME_TO="${DASHBOARD_TIME_TO:-now}"
DASHBOARD_URL="$KIBANA_URL/app/dashboards#/view/$DASHBOARD_ID?_g=(filters:!(),refreshInterval:(pause:!f,value:5000),time:(from:'$DASHBOARD_TIME_FROM',to:'$DASHBOARD_TIME_TO'))"

wait_for_kibana() {
  local attempt
  for attempt in $(seq 1 60); do
    local state
    state="$(curl -fsS "$KIBANA_URL/api/status" 2>/dev/null | jq -r '.status.overall.state // .status.overall.level // empty' || true)"
    if [[ "$state" == "green" || "$state" == "yellow" || "$state" == "available" || "$state" == "degraded" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for Kibana at $KIBANA_URL" >&2
  return 1
}

import_saved_objects() {
  if [[ ! -f "$DASHBOARD_DEFINITION" ]]; then
    echo "Dashboard definition not found: $DASHBOARD_DEFINITION" >&2
    return 1
  fi

  curl -fsS \
    --retry 10 \
    --retry-delay 2 \
    --retry-all-errors \
    -X POST "$KIBANA_URL/api/saved_objects/_import?overwrite=true" \
    -H "kbn-xsrf: kafka-connect-demo" \
    --form "file=@$DASHBOARD_DEFINITION" >/dev/null
}

set_dashboard_time_window() {
  local body
  local current
  body="$(mktemp)"
  current="$(mktemp)"

  curl -fsS "$KIBANA_URL/api/saved_objects/dashboard/$DASHBOARD_ID" \
    -H "kbn-xsrf: kafka-connect-demo" >"$current"

  jq \
    --arg time_from "$DASHBOARD_TIME_FROM" \
    --arg time_to "$DASHBOARD_TIME_TO" \
    '{
      attributes: (
        .attributes
        + {
          timeRestore: true,
          timeFrom: $time_from,
          timeTo: $time_to,
          refreshInterval: {
            pause: false,
            value: 5000
          }
        }
      ),
      references: (.references // [])
    }' "$current" >"$body"

  curl -fsS \
    --retry 10 \
    --retry-delay 2 \
    --retry-all-errors \
    -X POST "$KIBANA_URL/api/saved_objects/dashboard/$DASHBOARD_ID?overwrite=true" \
    -H "Content-Type: application/json" \
    -H "kbn-xsrf: kafka-connect-demo" \
    --data-binary "@$body" >/dev/null
  rm -f "$body" "$current"
}

wait_for_kibana
import_saved_objects
set_dashboard_time_window

echo "Imported Kibana dashboard definition: $DASHBOARD_DEFINITION"
echo "Dashboard: $DASHBOARD_URL"
