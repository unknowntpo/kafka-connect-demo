#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

assert_eq() {
  local expected="$1"
  local actual="$2"
  local message="$3"
  if [[ "$expected" != "$actual" ]]; then
    echo "Assertion failed: $message. Expected '$expected' but got '$actual'." >&2
    exit 1
  fi
}

wait_for_index_count() {
  local expected="$1"
  local attempt
  for attempt in $(seq 1 60); do
    local actual
    actual="$(curl -fsS http://localhost:9200/product-events/_count 2>/dev/null | jq -r '.count // empty' || true)"
    actual="${actual//$'\r'/}"
    if [[ "$actual" =~ ^[0-9]+$ ]] && (( actual >= expected )); then
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for at least $expected indexed documents" >&2
  return 1
}

wait_for_connector_running() {
  local connector="$1"
  local attempt
  for attempt in $(seq 1 60); do
    local all_running
    all_running="$(curl -fsS "http://localhost:8083/connectors/$connector/status" 2>/dev/null \
      | jq -r '(.connector.state == "RUNNING") and ((.tasks | length) > 0) and all(.tasks[]; .state == "RUNNING")' 2>/dev/null || true)"
    if [[ "$all_running" == "true" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for connector $connector and all tasks to reach RUNNING" >&2
  return 1
}

assert_topic_contains() {
  local pattern="$1"
  local output
  output="$("$ROOT_DIR/scripts/inspect-topic.sh" product.events 200 10000)"
  if [[ "$output" != *"$pattern"* ]]; then
    echo "Topic output did not contain expected pattern: $pattern" >&2
    echo "$output" >&2
    exit 1
  fi
}

cd "$ROOT_DIR"

"$ROOT_DIR/scripts/reset.sh"
"$ROOT_DIR/scripts/start.sh"
"$ROOT_DIR/scripts/wait-for-connect.sh"

plugin_names="$(curl -fsS http://localhost:8083/connector-plugins | jq -r '.[].class')"
if [[ "$plugin_names" != *"io.confluent.connect.elasticsearch.ElasticsearchSinkConnector"* ]]; then
  echo "Elasticsearch sink connector plugin is missing" >&2
  exit 1
fi

"$ROOT_DIR/scripts/create-topics.sh"
"$ROOT_DIR/scripts/create-search-resources.sh"
"$ROOT_DIR/scripts/register-connectors.sh"
wait_for_connector_running elasticsearch-sink-product-events

GRADLE_DOCKER_NETWORK=kafka-connect-demo_default KAFKA_BOOTSTRAP_SERVERS=broker:29092 \
  "$ROOT_DIR/scripts/run-gradle.sh" --no-daemon run --args="generate --rate-per-second=80 --duration-seconds=5 --initial-stock=40 --seed=42 --malformed-ratio=0.02"
wait_for_index_count 100

indexed_count="$(curl -fsS http://localhost:9200/product-events/_count | jq -r '.count')"
if (( indexed_count < 100 )); then
  echo "Expected at least 100 indexed product events, got $indexed_count" >&2
  exit 1
fi

assert_topic_contains "PURCHASE_SUCCEEDED"

stock_docs="$(curl -fsS "http://localhost:9200/product-events/_search" -H "Content-Type: application/json" -d '{"size":0,"query":{"term":{"event_type":"PURCHASE_FAILED"}}}' | jq -r '.hits.total.value')"
if (( stock_docs < 1 )); then
  echo "Expected at least one PURCHASE_FAILED document" >&2
  exit 1
fi

pipeline_docs="$(curl -fsS "http://localhost:9200/product-events/_search" -H "Content-Type: application/json" -d '{"size":0,"query":{"term":{"pipeline":"connect-search-demo"}}}' | jq -r '.hits.total.value')"
if (( pipeline_docs < 100 )); then
  echo "Expected SMT pipeline metadata in indexed documents" >&2
  exit 1
fi

region_docs="$(curl -fsS "http://localhost:9200/product-events/_search" -H "Content-Type: application/json" -d '{"size":0,"query":{"exists":{"field":"metadata_region"}}}' | jq -r '.hits.total.value')"
if (( region_docs < 100 )); then
  echo "Expected SMT-flattened metadata_region in indexed documents" >&2
  exit 1
fi

dlq_offsets="$(docker compose exec -T broker kafka-run-class kafka.tools.GetOffsetShell --broker-list broker:29092 --topic product.events.dlq 2>/dev/null | awk -F ':' '{sum += $3} END {print sum + 0}')"
if (( dlq_offsets < 1 )); then
  echo "Expected malformed records to be written to product.events.dlq" >&2
  exit 1
fi

docker compose restart connect >/dev/null
"$ROOT_DIR/scripts/wait-for-connect.sh"
wait_for_connector_running elasticsearch-sink-product-events

GRADLE_DOCKER_NETWORK=kafka-connect-demo_default KAFKA_BOOTSTRAP_SERVERS=broker:29092 \
  "$ROOT_DIR/scripts/run-gradle.sh" --no-daemon run --args="generate --rate-per-second=20 --duration-seconds=2 --initial-stock=10 --seed=99 --malformed-ratio=0"
wait_for_index_count "$((indexed_count + 1))"

final_count="$(curl -fsS http://localhost:9200/product-events/_count | jq -r '.count')"
if (( final_count <= indexed_count )); then
  echo "Expected more documents after Connect restart; before=$indexed_count after=$final_count" >&2
  exit 1
fi

internal_topics="$(docker compose exec -T broker kafka-topics --bootstrap-server broker:29092 --list | tr -d '\r')"
if [[ "$internal_topics" != *"connect-configs-hot-product-demo"* ]]; then
  echo "connect-configs-hot-product-demo topic missing" >&2
  exit 1
fi
if [[ "$internal_topics" != *"connect-offsets-hot-product-demo"* ]]; then
  echo "connect-offsets-hot-product-demo topic missing" >&2
  exit 1
fi
if [[ "$internal_topics" != *"connect-status-hot-product-demo"* ]]; then
  echo "connect-status-hot-product-demo topic missing" >&2
  exit 1
fi

echo "E2E pipeline verification passed."
