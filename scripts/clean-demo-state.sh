#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONNECTOR_NAME="${CONNECTOR_NAME:-elasticsearch-sink-product-events}"
TOPICS=(
  "product.events"
  "product.events.dlq"
  "connect-configs-hot-product-demo"
  "connect-offsets-hot-product-demo"
  "connect-status-hot-product-demo"
)

wait_for_broker() {
  local attempt
  for attempt in $(seq 1 60); do
    if docker compose exec -T broker kafka-topics --bootstrap-server broker:29092 --list >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for Kafka broker" >&2
  return 1
}

wait_for_elasticsearch() {
  local attempt
  for attempt in $(seq 1 60); do
    if curl -fsS http://localhost:9200/_cluster/health >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for Elasticsearch" >&2
  return 1
}

clear_elasticsearch_readonly_blocks() {
  curl -fsS -X PUT "http://localhost:9200/_all/_settings" \
    -H "Content-Type: application/json" \
    -d '{"index.blocks.read_only_allow_delete": null}' >/dev/null 2>&1 || true
}

topic_exists() {
  local topic="$1"
  docker compose exec -T broker kafka-topics \
    --bootstrap-server broker:29092 \
    --list | tr -d '\r' | grep -Fxq "$topic"
}

wait_topic_deleted() {
  local topic="$1"
  local attempt
  for attempt in $(seq 1 30); do
    if ! topic_exists "$topic"; then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for topic deletion: $topic" >&2
  return 1
}

cd "$ROOT_DIR"

docker compose up -d broker elasticsearch >/dev/null
wait_for_broker
wait_for_elasticsearch
clear_elasticsearch_readonly_blocks

curl -fsS -X DELETE "http://localhost:8083/connectors/$CONNECTOR_NAME" >/dev/null 2>&1 || true
docker compose stop connect >/dev/null 2>&1 || true

for topic in "${TOPICS[@]}"; do
  docker compose exec -T broker kafka-topics \
    --bootstrap-server broker:29092 \
    --delete \
    --if-exists \
    --topic "$topic" >/dev/null 2>&1 || true
done

for topic in "${TOPICS[@]}"; do
  wait_topic_deleted "$topic"
done

curl -fsS -X DELETE "http://localhost:9200/product-events" >/dev/null 2>&1 || true

docker compose up -d connect kibana redpanda-console >/dev/null
"$ROOT_DIR/scripts/wait-for-connect.sh"

echo "Demo state cleaned: connector, Kafka topics, Connect internal topics, and Elasticsearch index removed."
