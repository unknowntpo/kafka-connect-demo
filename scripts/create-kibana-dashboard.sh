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
    "title": "熱門商品 - 事件總數",
    "visState": "{\"title\":\"熱門商品 - 事件總數\",\"type\":\"metric\",\"params\":{\"addTooltip\":true,\"addLegend\":false,\"type\":\"metric\",\"metric\":{\"percentageMode\":false,\"useRanges\":false,\"colorSchema\":\"Green to Red\",\"metricColorMode\":\"None\",\"colorsRange\":[{\"from\":0,\"to\":10000}],\"labels\":{\"show\":true},\"style\":{\"bgFill\":\"#000\",\"bgColor\":false,\"labelColor\":false,\"subText\":\"Kafka Connect 已寫入的事件\",\"fontSize\":48}}},\"aggs\":[{\"id\":\"1\",\"enabled\":true,\"type\":\"count\",\"schema\":\"metric\",\"params\":{}}]}",
    "uiStateJSON": "{}",
    "description": "Kafka Connect 已寫入 Elasticsearch 的商品事件總數。",
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
    "title": "熱門商品 - 事件類型趨勢",
    "visState": "{\"title\":\"熱門商品 - 事件類型趨勢\",\"type\":\"line\",\"params\":{\"type\":\"line\",\"grid\":{\"categoryLines\":false},\"categoryAxes\":[{\"id\":\"CategoryAxis-1\",\"type\":\"category\",\"position\":\"bottom\",\"show\":true,\"style\":{},\"scale\":{\"type\":\"linear\"},\"labels\":{\"show\":true,\"truncate\":100},\"title\":{}}],\"valueAxes\":[{\"id\":\"ValueAxis-1\",\"name\":\"LeftAxis-1\",\"type\":\"value\",\"position\":\"left\",\"show\":true,\"style\":{},\"scale\":{\"type\":\"linear\",\"mode\":\"normal\"},\"labels\":{\"show\":true,\"rotate\":0,\"filter\":false,\"truncate\":100},\"title\":{\"text\":\"事件數\"}}],\"seriesParams\":[{\"show\":true,\"type\":\"line\",\"mode\":\"normal\",\"data\":{\"label\":\"Count\",\"id\":\"1\"},\"valueAxis\":\"ValueAxis-1\",\"drawLinesBetweenPoints\":true,\"showCircles\":true}],\"addTooltip\":true,\"addLegend\":true,\"legendPosition\":\"right\",\"times\":[],\"addTimeMarker\":false},\"aggs\":[{\"id\":\"1\",\"enabled\":true,\"type\":\"count\",\"schema\":\"metric\",\"params\":{}},{\"id\":\"2\",\"enabled\":true,\"type\":\"date_histogram\",\"schema\":\"segment\",\"params\":{\"field\":\"occurred_at\",\"timeRange\":{\"from\":\"now-90m\",\"to\":\"now\"},\"useNormalizedEsInterval\":true,\"interval\":\"auto\",\"drop_partials\":false,\"min_doc_count\":1,\"extended_bounds\":{}}},{\"id\":\"3\",\"enabled\":true,\"type\":\"terms\",\"schema\":\"group\",\"params\":{\"field\":\"event_type\",\"orderBy\":\"1\",\"order\":\"desc\",\"size\":5,\"otherBucket\":false,\"missingBucket\":false}}]}",
    "uiStateJSON": "{}",
    "description": "依事件類型切分的時間序列趨勢，用來觀察瀏覽、點擊、成功與失敗事件。",
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
    "title": "熱門商品 - 業務結果",
    "visState": "{\"title\":\"熱門商品 - 業務結果\",\"type\":\"table\",\"params\":{\"perPage\":10,\"showPartialRows\":false,\"showMetricsAtAllLevels\":false,\"showTotal\":true,\"totalFunc\":\"sum\"},\"aggs\":[{\"id\":\"1\",\"enabled\":true,\"type\":\"count\",\"schema\":\"metric\",\"params\":{}},{\"id\":\"2\",\"enabled\":true,\"type\":\"filters\",\"schema\":\"bucket\",\"params\":{\"filters\":[{\"input\":{\"query\":\"event_type: (PURCHASE_SUCCEEDED or COUPON_CLAIM_SUCCEEDED)\",\"language\":\"kuery\"},\"label\":\"成功\"},{\"input\":{\"query\":\"event_type: (PURCHASE_FAILED or COUPON_CLAIM_FAILED)\",\"language\":\"kuery\"},\"label\":\"失敗\"},{\"input\":{\"query\":\"event_type: (BUY_CLICKED or PAGE_REFRESHED)\",\"language\":\"kuery\"},\"label\":\"需求壓力\"}]}}]}",
    "uiStateJSON": "{}",
    "description": "商品或折價券流程中的成功、失敗與需求壓力統計。",
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
    "title": "熱門商品 - 失敗原因",
    "visState": "{\"title\":\"熱門商品 - 失敗原因\",\"type\":\"table\",\"params\":{\"perPage\":10,\"showPartialRows\":false,\"showMetricsAtAllLevels\":false,\"showTotal\":false,\"totalFunc\":\"sum\"},\"aggs\":[{\"id\":\"1\",\"enabled\":true,\"type\":\"count\",\"schema\":\"metric\",\"params\":{}},{\"id\":\"2\",\"enabled\":true,\"type\":\"terms\",\"schema\":\"bucket\",\"params\":{\"field\":\"failure_reason\",\"orderBy\":\"1\",\"order\":\"desc\",\"size\":10,\"otherBucket\":false,\"missingBucket\":false}}]}",
    "uiStateJSON": "{}",
    "description": "使用者購買或領券失敗的原因，例如售罄、限流或付款失敗。",
    "version": 1,
    "kibanaSavedObjectMeta": {
      "searchSourceJSON": "{\"query\":{\"query\":\"event_type: (PURCHASE_FAILED or COUPON_CLAIM_FAILED)\",\"language\":\"kuery\"},\"filter\":[],\"indexRefName\":\"kibanaSavedObjectMeta.searchSourceJSON.index\"}"
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
    "title": "熱門商品 - 活躍使用者",
    "visState": "{\"title\":\"熱門商品 - 活躍使用者\",\"type\":\"table\",\"params\":{\"perPage\":10,\"showPartialRows\":false,\"showMetricsAtAllLevels\":false,\"showTotal\":false,\"totalFunc\":\"sum\"},\"aggs\":[{\"id\":\"1\",\"enabled\":true,\"type\":\"count\",\"schema\":\"metric\",\"params\":{}},{\"id\":\"2\",\"enabled\":true,\"type\":\"terms\",\"schema\":\"bucket\",\"params\":{\"field\":\"user_id\",\"orderBy\":\"1\",\"order\":\"desc\",\"size\":10,\"otherBucket\":false,\"missingBucket\":false}}]}",
    "uiStateJSON": "{}",
    "description": "依事件數排序的活躍使用者。",
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
    "title": "熱門商品 - 地區流量",
    "visState": "{\"title\":\"熱門商品 - 地區流量\",\"type\":\"table\",\"params\":{\"perPage\":10,\"showPartialRows\":false,\"showMetricsAtAllLevels\":false,\"showTotal\":false,\"totalFunc\":\"sum\"},\"aggs\":[{\"id\":\"1\",\"enabled\":true,\"type\":\"count\",\"schema\":\"metric\",\"params\":{}},{\"id\":\"2\",\"enabled\":true,\"type\":\"terms\",\"schema\":\"bucket\",\"params\":{\"field\":\"metadata_region\",\"orderBy\":\"1\",\"order\":\"desc\",\"size\":10,\"otherBucket\":false,\"missingBucket\":false}}]}",
    "uiStateJSON": "{}",
    "description": "由 Kafka Connect Flatten SMT 產生的地區流量切分。",
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
    "title": "熱門商品銷售觀測",
    "description": "Kafka Connect demo dashboard：觀察熱門商品或限量折價券的流量、成功、失敗與庫存壓力。",
    "hits": 0,
    "optionsJSON": "{\"useMargins\":true,\"syncColors\":false,\"hidePanelTitles\":false}",
    "panelsJSON": "[{\"version\":\"7.17.23\",\"type\":\"visualization\",\"gridData\":{\"x\":0,\"y\":0,\"w\":12,\"h\":8,\"i\":\"1\"},\"panelIndex\":\"1\",\"embeddableConfig\":{},\"panelRefName\":\"panel_1\"},{\"version\":\"7.17.23\",\"type\":\"visualization\",\"gridData\":{\"x\":12,\"y\":0,\"w\":36,\"h\":18,\"i\":\"2\"},\"panelIndex\":\"2\",\"embeddableConfig\":{},\"panelRefName\":\"panel_2\"},{\"version\":\"7.17.23\",\"type\":\"visualization\",\"gridData\":{\"x\":0,\"y\":8,\"w\":12,\"h\":10,\"i\":\"3\"},\"panelIndex\":\"3\",\"embeddableConfig\":{},\"panelRefName\":\"panel_3\"},{\"version\":\"7.17.23\",\"type\":\"visualization\",\"gridData\":{\"x\":0,\"y\":18,\"w\":24,\"h\":12,\"i\":\"4\"},\"panelIndex\":\"4\",\"embeddableConfig\":{},\"panelRefName\":\"panel_4\"},{\"version\":\"7.17.23\",\"type\":\"visualization\",\"gridData\":{\"x\":24,\"y\":18,\"w\":24,\"h\":12,\"i\":\"5\"},\"panelIndex\":\"5\",\"embeddableConfig\":{},\"panelRefName\":\"panel_5\"},{\"version\":\"7.17.23\",\"type\":\"visualization\",\"gridData\":{\"x\":0,\"y\":30,\"w\":48,\"h\":10,\"i\":\"6\"},\"panelIndex\":\"6\",\"embeddableConfig\":{},\"panelRefName\":\"panel_6\"}]",
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
