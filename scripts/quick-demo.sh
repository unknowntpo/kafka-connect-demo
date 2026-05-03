#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONNECT_URL="${CONNECT_URL:-http://localhost:${CONNECT_HOST_PORT:-18083}}"
CONSOLE_URL="${CONSOLE_URL:-http://localhost:${REDPANDA_CONSOLE_HOST_PORT:-18080}}"
TOPIC="${TOPIC:-quick.orders}"
CONNECTOR_NAME="${CONNECTOR_NAME:-file-sink-quick-demo}"
SINK_FILE="$ROOT_DIR/sink/quick-orders.jsonl"

wait_for_connector_running() {
  local attempt
  for attempt in $(seq 1 60); do
    local all_running
    all_running="$(curl -fsS "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" 2>/dev/null \
      | jq -r '(.connector.state == "RUNNING") and ((.tasks | length) > 0) and all(.tasks[]; .state == "RUNNING")' 2>/dev/null || true)"
    if [[ "$all_running" == "true" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for connector $CONNECTOR_NAME to reach RUNNING" >&2
  curl -fsS "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq . >&2 || true
  return 1
}

wait_for_sink_lines() {
  local expected="$1"
  local attempt
  for attempt in $(seq 1 30); do
    local actual=0
    if [[ -f "$SINK_FILE" ]]; then
      actual="$(wc -l < "$SINK_FILE" | tr -d ' ')"
    fi
    if (( actual >= expected )); then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for $expected records in $SINK_FILE" >&2
  return 1
}

topic_exists() {
  docker compose exec -T broker kafka-topics \
    --bootstrap-server broker:29092 \
    --list | tr -d '\r' | grep -Fxq "$TOPIC"
}

wait_topic_deleted() {
  local attempt
  for attempt in $(seq 1 30); do
    if ! topic_exists; then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for topic deletion: $TOPIC" >&2
  return 1
}

cd "$ROOT_DIR"

"$ROOT_DIR/scripts/start.sh" broker connect redpanda-console
WAIT_FOR_ELASTICSEARCH=0 "$ROOT_DIR/scripts/wait-for-connect.sh"

curl -fsS -X DELETE "$CONNECT_URL/connectors/$CONNECTOR_NAME" >/dev/null 2>&1 || true
docker compose exec -T broker kafka-topics \
  --bootstrap-server broker:29092 \
  --delete \
  --if-exists \
  --topic "$TOPIC" >/dev/null 2>&1 || true
wait_topic_deleted

rm -f "$SINK_FILE"
docker compose exec -T broker kafka-topics \
  --bootstrap-server broker:29092 \
  --create \
  --if-not-exists \
  --topic "$TOPIC" \
  --partitions 1 \
  --replication-factor 1 >/dev/null

"$ROOT_DIR/scripts/register-connectors.sh" "$ROOT_DIR/connectors/file-sink-quick-demo.json" >/dev/null
wait_for_connector_running

printf '%s\n' \
  '{"order_id":"demo-001","user_id":"u-001","amount":399,"status":"PAID"}' \
  '{"order_id":"demo-002","user_id":"u-002","amount":1280,"status":"PAID"}' \
  '{"order_id":"demo-003","user_id":"u-003","amount":99,"status":"CANCELLED"}' \
  | docker compose exec -T broker kafka-console-producer \
      --bootstrap-server broker:29092 \
      --topic "$TOPIC" >/dev/null

wait_for_sink_lines 3

echo "Kafka Connect quick demo is ready."
echo "Redpanda Console: $CONSOLE_URL"
echo "Kafka Connect REST: $CONNECT_URL"
echo "Topic: $TOPIC"
echo "Connector: $CONNECTOR_NAME"
echo "Sink file: $SINK_FILE"
echo
cat "$SINK_FILE"
