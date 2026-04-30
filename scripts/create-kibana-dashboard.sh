#!/usr/bin/env bash
set -euo pipefail

KIBANA_URL="${KIBANA_URL:-http://localhost:5601}"
INDEX_PATTERN_ID="product-events-data-view"
DASHBOARD_ID="hot-product-sales-dashboard"
DASHBOARD_TIME_FROM="${DASHBOARD_TIME_FROM:-now-90m}"
DASHBOARD_TIME_TO="${DASHBOARD_TIME_TO:-now}"

wait_for_kibana() {
  local attempt
  for attempt in $(seq 1 60); do
    local state
    state="$(curl -fsS "$KIBANA_URL/api/status" 2>/dev/null | jq -r '.status.overall.state // .status.overall.level // empty' || true)"
    if [[ "$state" == "green" || "$state" == "available" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for Kibana at $KIBANA_URL" >&2
  return 1
}

put_saved_object() {
  local type="$1"
  local id="$2"
  local body_file="$3"
  curl -fsS \
    -X POST "$KIBANA_URL/api/saved_objects/$type/$id?overwrite=true" \
    -H "Content-Type: application/json" \
    -H "kbn-xsrf: kafka-connect-demo" \
    --data-binary "@$body_file" >/dev/null
}

create_index_pattern() {
  local body
  body="$(mktemp)"
  cat >"$body" <<JSON
{
  "attributes": {
    "title": "product-events",
    "timeFieldName": "occurred_at"
  }
}
JSON
  put_saved_object "index-pattern" "$INDEX_PATTERN_ID" "$body"
  rm -f "$body"
}

create_total_events_metric() {
  local body
  body="$(mktemp)"
  cat >"$body" <<'JSON'
{
  "attributes": {
    "title": "Hot Product - Total Events",
    "visState": "{\"title\":\"Hot Product - Total Events\",\"type\":\"metric\",\"params\":{\"addTooltip\":true,\"addLegend\":false,\"type\":\"metric\",\"metric\":{\"percentageMode\":false,\"useRanges\":false,\"colorSchema\":\"Green to Red\",\"metricColorMode\":\"None\",\"colorsRange\":[{\"from\":0,\"to\":10000}],\"labels\":{\"show\":true},\"style\":{\"bgFill\":\"#000\",\"bgColor\":false,\"labelColor\":false,\"subText\":\"events indexed by Kafka Connect\",\"fontSize\":48}}},\"aggs\":[{\"id\":\"1\",\"enabled\":true,\"type\":\"count\",\"schema\":\"metric\",\"params\":{}}]}",
    "uiStateJSON": "{}",
    "description": "Total product events indexed into Elasticsearch by Kafka Connect.",
    "version": 1,
    "kibanaSavedObjectMeta": {
      "searchSourceJSON": "{\"query\":{\"query\":\"\",\"language\":\"kuery\"},\"filter\":[],\"indexRefName\":\"kibanaSavedObjectMeta.searchSourceJSON.index\"}"
    }
  },
  "references": [
    {
      "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
      "type": "index-pattern",
      "id": "product-events-data-view"
    }
  ]
}
JSON
  put_saved_object "visualization" "hot-product-total-events" "$body"
  rm -f "$body"
}

create_event_volume_line() {
  local body
  body="$(mktemp)"
  cat >"$body" <<'JSON'
{
  "attributes": {
    "title": "Hot Product - Event Volume by Type",
    "visState": "{\"title\":\"Hot Product - Event Volume by Type\",\"type\":\"line\",\"params\":{\"type\":\"line\",\"grid\":{\"categoryLines\":false},\"categoryAxes\":[{\"id\":\"CategoryAxis-1\",\"type\":\"category\",\"position\":\"bottom\",\"show\":true,\"style\":{},\"scale\":{\"type\":\"linear\"},\"labels\":{\"show\":true,\"truncate\":100},\"title\":{}}],\"valueAxes\":[{\"id\":\"ValueAxis-1\",\"name\":\"LeftAxis-1\",\"type\":\"value\",\"position\":\"left\",\"show\":true,\"style\":{},\"scale\":{\"type\":\"linear\",\"mode\":\"normal\"},\"labels\":{\"show\":true,\"rotate\":0,\"filter\":false,\"truncate\":100},\"title\":{\"text\":\"Events\"}}],\"seriesParams\":[{\"show\":true,\"type\":\"line\",\"mode\":\"normal\",\"data\":{\"label\":\"Count\",\"id\":\"1\"},\"valueAxis\":\"ValueAxis-1\",\"drawLinesBetweenPoints\":true,\"showCircles\":true}],\"addTooltip\":true,\"addLegend\":true,\"legendPosition\":\"right\",\"times\":[],\"addTimeMarker\":false},\"aggs\":[{\"id\":\"1\",\"enabled\":true,\"type\":\"count\",\"schema\":\"metric\",\"params\":{}},{\"id\":\"2\",\"enabled\":true,\"type\":\"date_histogram\",\"schema\":\"segment\",\"params\":{\"field\":\"occurred_at\",\"timeRange\":{\"from\":\"now-90m\",\"to\":\"now\"},\"useNormalizedEsInterval\":true,\"interval\":\"auto\",\"drop_partials\":false,\"min_doc_count\":1,\"extended_bounds\":{}}},{\"id\":\"3\",\"enabled\":true,\"type\":\"terms\",\"schema\":\"group\",\"params\":{\"field\":\"event_type\",\"orderBy\":\"1\",\"order\":\"desc\",\"size\":5,\"otherBucket\":false,\"missingBucket\":false}}]}",
    "uiStateJSON": "{}",
    "description": "Time-series trend for product views, buy clicks, successful purchases, and failed purchases.",
    "version": 1,
    "kibanaSavedObjectMeta": {
      "searchSourceJSON": "{\"query\":{\"query\":\"\",\"language\":\"kuery\"},\"filter\":[],\"indexRefName\":\"kibanaSavedObjectMeta.searchSourceJSON.index\"}"
    }
  },
  "references": [
    {
      "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
      "type": "index-pattern",
      "id": "product-events-data-view"
    }
  ]
}
JSON
  put_saved_object "visualization" "hot-product-event-volume-by-type" "$body"
  rm -f "$body"
}

create_purchase_outcome_table() {
  local body
  body="$(mktemp)"
  cat >"$body" <<'JSON'
{
  "attributes": {
    "title": "Hot Product - Business Outcomes",
    "visState": "{\"title\":\"Hot Product - Business Outcomes\",\"type\":\"table\",\"params\":{\"perPage\":10,\"showPartialRows\":false,\"showMetricsAtAllLevels\":false,\"showTotal\":true,\"totalFunc\":\"sum\"},\"aggs\":[{\"id\":\"1\",\"enabled\":true,\"type\":\"count\",\"schema\":\"metric\",\"params\":{}},{\"id\":\"2\",\"enabled\":true,\"type\":\"filters\",\"schema\":\"bucket\",\"params\":{\"filters\":[{\"input\":{\"query\":\"event_type: (PURCHASE_SUCCEEDED or COUPON_CLAIM_SUCCEEDED)\",\"language\":\"kuery\"},\"label\":\"Succeeded\"},{\"input\":{\"query\":\"event_type: (PURCHASE_FAILED or COUPON_CLAIM_FAILED)\",\"language\":\"kuery\"},\"label\":\"Failed\"},{\"input\":{\"query\":\"event_type: (BUY_CLICKED or PAGE_REFRESHED)\",\"language\":\"kuery\"},\"label\":\"Demand pressure\"}]}}]}",
    "uiStateJSON": "{}",
    "description": "Business-level outcome table for the hot product funnel.",
    "version": 1,
    "kibanaSavedObjectMeta": {
      "searchSourceJSON": "{\"query\":{\"query\":\"\",\"language\":\"kuery\"},\"filter\":[],\"indexRefName\":\"kibanaSavedObjectMeta.searchSourceJSON.index\"}"
    }
  },
  "references": [
    {
      "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
      "type": "index-pattern",
      "id": "product-events-data-view"
    }
  ]
}
JSON
  put_saved_object "visualization" "hot-product-purchase-outcomes" "$body"
  rm -f "$body"
}

create_failure_reason_table() {
  local body
  body="$(mktemp)"
  cat >"$body" <<'JSON'
{
  "attributes": {
    "title": "Hot Product - Failure Reasons",
    "visState": "{\"title\":\"Hot Product - Failure Reasons\",\"type\":\"table\",\"params\":{\"perPage\":10,\"showPartialRows\":false,\"showMetricsAtAllLevels\":false,\"showTotal\":false,\"totalFunc\":\"sum\"},\"aggs\":[{\"id\":\"1\",\"enabled\":true,\"type\":\"count\",\"schema\":\"metric\",\"params\":{}},{\"id\":\"2\",\"enabled\":true,\"type\":\"terms\",\"schema\":\"bucket\",\"params\":{\"field\":\"failure_reason\",\"orderBy\":\"1\",\"order\":\"desc\",\"size\":10,\"otherBucket\":false,\"missingBucket\":false}}]}",
    "uiStateJSON": "{}",
    "description": "Why users fail to buy the hot product, especially OUT_OF_STOCK.",
    "version": 1,
    "kibanaSavedObjectMeta": {
      "searchSourceJSON": "{\"query\":{\"query\":\"event_type: PURCHASE_FAILED\",\"language\":\"kuery\"},\"filter\":[],\"indexRefName\":\"kibanaSavedObjectMeta.searchSourceJSON.index\"}"
    }
  },
  "references": [
    {
      "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
      "type": "index-pattern",
      "id": "product-events-data-view"
    }
  ]
}
JSON
  put_saved_object "visualization" "hot-product-failure-reasons" "$body"
  rm -f "$body"
}

create_active_users_table() {
  local body
  body="$(mktemp)"
  cat >"$body" <<'JSON'
{
  "attributes": {
    "title": "Hot Product - Active Users",
    "visState": "{\"title\":\"Hot Product - Active Users\",\"type\":\"table\",\"params\":{\"perPage\":10,\"showPartialRows\":false,\"showMetricsAtAllLevels\":false,\"showTotal\":false,\"totalFunc\":\"sum\"},\"aggs\":[{\"id\":\"1\",\"enabled\":true,\"type\":\"count\",\"schema\":\"metric\",\"params\":{}},{\"id\":\"2\",\"enabled\":true,\"type\":\"terms\",\"schema\":\"bucket\",\"params\":{\"field\":\"user_id\",\"orderBy\":\"1\",\"order\":\"desc\",\"size\":10,\"otherBucket\":false,\"missingBucket\":false}}]}",
    "uiStateJSON": "{}",
    "description": "Top users by generated product activity.",
    "version": 1,
    "kibanaSavedObjectMeta": {
      "searchSourceJSON": "{\"query\":{\"query\":\"\",\"language\":\"kuery\"},\"filter\":[],\"indexRefName\":\"kibanaSavedObjectMeta.searchSourceJSON.index\"}"
    }
  },
  "references": [
    {
      "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
      "type": "index-pattern",
      "id": "product-events-data-view"
    }
  ]
}
JSON
  put_saved_object "visualization" "hot-product-active-users" "$body"
  rm -f "$body"
}

create_region_table() {
  local body
  body="$(mktemp)"
  cat >"$body" <<'JSON'
{
  "attributes": {
    "title": "Hot Product - Traffic by Region",
    "visState": "{\"title\":\"Hot Product - Traffic by Region\",\"type\":\"table\",\"params\":{\"perPage\":10,\"showPartialRows\":false,\"showMetricsAtAllLevels\":false,\"showTotal\":false,\"totalFunc\":\"sum\"},\"aggs\":[{\"id\":\"1\",\"enabled\":true,\"type\":\"count\",\"schema\":\"metric\",\"params\":{}},{\"id\":\"2\",\"enabled\":true,\"type\":\"terms\",\"schema\":\"bucket\",\"params\":{\"field\":\"metadata_region\",\"orderBy\":\"1\",\"order\":\"desc\",\"size\":10,\"otherBucket\":false,\"missingBucket\":false}}]}",
    "uiStateJSON": "{}",
    "description": "Regional traffic split produced by the Kafka Connect Flatten SMT.",
    "version": 1,
    "kibanaSavedObjectMeta": {
      "searchSourceJSON": "{\"query\":{\"query\":\"\",\"language\":\"kuery\"},\"filter\":[],\"indexRefName\":\"kibanaSavedObjectMeta.searchSourceJSON.index\"}"
    }
  },
  "references": [
    {
      "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
      "type": "index-pattern",
      "id": "product-events-data-view"
    }
  ]
}
JSON
  put_saved_object "visualization" "hot-product-traffic-by-region" "$body"
  rm -f "$body"
}

create_dashboard() {
  local body
  body="$(mktemp)"
  cat >"$body" <<JSON
{
  "attributes": {
    "title": "Hot Product Sales Observability",
    "description": "Kafka Connect demo dashboard: one hot product is receiving traffic, purchases, failures, and stock pressure.",
    "hits": 0,
    "optionsJSON": "{\"useMargins\":true,\"syncColors\":false,\"hidePanelTitles\":false}",
    "panelsJSON": "[{\"version\":\"7.17.23\",\"type\":\"visualization\",\"gridData\":{\"x\":0,\"y\":0,\"w\":12,\"h\":8,\"i\":\"1\"},\"panelIndex\":\"1\",\"embeddableConfig\":{},\"panelRefName\":\"panel_1\"},{\"version\":\"7.17.23\",\"type\":\"visualization\",\"gridData\":{\"x\":12,\"y\":0,\"w\":36,\"h\":16,\"i\":\"2\"},\"panelIndex\":\"2\",\"embeddableConfig\":{},\"panelRefName\":\"panel_2\"},{\"version\":\"7.17.23\",\"type\":\"visualization\",\"gridData\":{\"x\":0,\"y\":8,\"w\":12,\"h\":16,\"i\":\"3\"},\"panelIndex\":\"3\",\"embeddableConfig\":{},\"panelRefName\":\"panel_3\"},{\"version\":\"7.17.23\",\"type\":\"visualization\",\"gridData\":{\"x\":0,\"y\":24,\"w\":24,\"h\":14,\"i\":\"4\"},\"panelIndex\":\"4\",\"embeddableConfig\":{},\"panelRefName\":\"panel_4\"},{\"version\":\"7.17.23\",\"type\":\"visualization\",\"gridData\":{\"x\":24,\"y\":24,\"w\":24,\"h\":14,\"i\":\"5\"},\"panelIndex\":\"5\",\"embeddableConfig\":{},\"panelRefName\":\"panel_5\"},{\"version\":\"7.17.23\",\"type\":\"visualization\",\"gridData\":{\"x\":0,\"y\":38,\"w\":24,\"h\":12,\"i\":\"6\"},\"panelIndex\":\"6\",\"embeddableConfig\":{},\"panelRefName\":\"panel_6\"}]",
    "timeRestore": true,
    "timeFrom": "$DASHBOARD_TIME_FROM",
    "timeTo": "$DASHBOARD_TIME_TO",
    "refreshInterval": {
      "pause": false,
      "value": 30000
    },
    "version": 1,
    "kibanaSavedObjectMeta": {
      "searchSourceJSON": "{\"query\":{\"query\":\"\",\"language\":\"kuery\"},\"filter\":[]}"
    }
  },
  "references": [
    {
      "name": "panel_1",
      "type": "visualization",
      "id": "hot-product-total-events"
    },
    {
      "name": "panel_2",
      "type": "visualization",
      "id": "hot-product-event-volume-by-type"
    },
    {
      "name": "panel_3",
      "type": "visualization",
      "id": "hot-product-purchase-outcomes"
    },
    {
      "name": "panel_4",
      "type": "visualization",
      "id": "hot-product-failure-reasons"
    },
    {
      "name": "panel_5",
      "type": "visualization",
      "id": "hot-product-active-users"
    },
    {
      "name": "panel_6",
      "type": "visualization",
      "id": "hot-product-traffic-by-region"
    }
  ]
}
JSON
  put_saved_object "dashboard" "$DASHBOARD_ID" "$body"
  rm -f "$body"
}

wait_for_kibana
create_index_pattern
create_total_events_metric
create_event_volume_line
create_purchase_outcome_table
create_failure_reason_table
create_active_users_table
create_region_table
create_dashboard

echo "Created Kibana dashboard: $KIBANA_URL/app/dashboards#/view/$DASHBOARD_ID"
