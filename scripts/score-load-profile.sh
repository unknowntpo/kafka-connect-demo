#!/usr/bin/env bash
set -euo pipefail

ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-http://localhost:9200}"
SCENARIO="${SCENARIO:-flash-sale-coupon}"
WINDOW="${WINDOW:-now-90m}"

query_file="$(mktemp)"
response_file="$(mktemp)"
trap 'rm -f "$query_file" "$response_file"' EXIT

cat >"$query_file" <<JSON
{
  "track_total_hits": true,
  "size": 0,
  "query": {
    "bool": {
      "filter": [
        { "term": { "scenario": "$SCENARIO" } },
        { "range": { "occurred_at": { "gte": "$WINDOW", "lte": "now" } } }
      ]
    }
  },
  "aggs": {
    "event_types": { "terms": { "field": "event_type", "size": 20 } },
    "failure_reasons": { "terms": { "field": "failure_reason", "size": 20 } },
    "phases": { "terms": { "field": "phase", "size": 10 } },
    "unique_users": { "cardinality": { "field": "user_id" } },
    "top_users": { "terms": { "field": "user_id", "size": 1 } },
    "min_inventory": { "min": { "field": "remaining_coupons" } },
    "pipeline_docs": { "filter": { "term": { "pipeline": "connect-search-demo" } } },
    "time_ranges": {
      "date_range": {
        "field": "occurred_at",
        "ranges": [
          { "key": "first_30m", "from": "now-90m", "to": "now-60m" },
          { "key": "middle_30m", "from": "now-60m", "to": "now-30m" },
          { "key": "last_30m", "from": "now-30m", "to": "now" }
        ]
      }
    }
  }
}
JSON

curl -fsS "$ELASTICSEARCH_URL/product-events/_search" \
  -H "Content-Type: application/json" \
  --data-binary "@$query_file" >"$response_file"

jq --arg scenario "$SCENARIO" '
  def bucket_count($name):
    ([.aggregations.event_types.buckets[]? | select(.key == $name) | .doc_count] | first) // 0;
  def reason_count($name):
    ([.aggregations.failure_reasons.buckets[]? | select(.key == $name) | .doc_count] | first) // 0;
  def range_count($name):
    ([.aggregations.time_ranges.buckets[]? | select(.key == $name) | .doc_count] | first) // 0;
  def score($condition; $points): if $condition then $points else 0 end;

  .hits.total.value as $total
  | bucket_count("PAGE_REFRESHED") as $refreshes
  | bucket_count("COUPON_VIEWED") as $views
  | bucket_count("WAITING_ROOM_JOINED") as $waiting
  | bucket_count("COUPON_CLAIM_SUCCEEDED") as $successes
  | bucket_count("COUPON_CLAIM_FAILED") as $failures
  | reason_count("COUPON_SOLD_OUT") as $sold_out
  | range_count("first_30m") as $first
  | range_count("last_30m") as $last
  | (.aggregations.unique_users.value // 0) as $unique_users
  | ((.aggregations.top_users.buckets[0].doc_count // 0) / (if $total == 0 then 1 else $total end)) as $top_user_share
  | (.aggregations.min_inventory.value // 999999) as $min_inventory
  | (.aggregations.pipeline_docs.doc_count // 0) as $pipeline_docs
  | {
      score: (
        score($total >= 20000; 20)
        + score(($last / (if $first == 0 then 1 else $first end)) >= 2; 20)
        + score($refreshes > $successes and $waiting > 0 and $views > 0; 15)
        + score($successes >= 1000 and $min_inventory == 0; 20)
        + score($failures > $successes and $sold_out > ($failures * 0.6); 15)
        + score($unique_users >= 8000 and $top_user_share < 0.01; 5)
        + score($pipeline_docs == $total; 5)
      ),
      scenario: $scenario,
      total_events: $total,
      first_30m_events: $first,
      last_30m_events: $last,
      surge_ratio: (($last / (if $first == 0 then 1 else $first end)) * 100 | round / 100),
      event_types: {
        page_refreshed: $refreshes,
        coupon_viewed: $views,
        waiting_room_joined: $waiting,
        coupon_claim_succeeded: $successes,
        coupon_claim_failed: $failures
      },
      failure_reasons: (.aggregations.failure_reasons.buckets | map({key, count: .doc_count})),
      min_remaining_coupons: $min_inventory,
      unique_users: $unique_users,
      top_user_share: (($top_user_share * 10000 | round) / 10000),
      pipeline_docs: $pipeline_docs,
      verdict: (if (
        $total >= 20000
        and (($last / (if $first == 0 then 1 else $first end)) >= 2)
        and $successes >= 1000
        and $min_inventory == 0
        and $failures > $successes
        and $sold_out > ($failures * 0.6)
        and $pipeline_docs == $total
      ) then "pass" else "needs_adjustment" end)
    }
' "$response_file"
