#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-broker:29092}"
GRADLE_DOCKER_NETWORK="${GRADLE_DOCKER_NETWORK:-kafka-connect-demo_default}"
RATE_PER_SECOND="${RATE_PER_SECOND:-4}"
DURATION_SECONDS="${DURATION_SECONDS:-3600}"
INITIAL_STOCK="${INITIAL_STOCK:-900}"
SEED="${SEED:-20260429}"
MALFORMED_RATIO="${MALFORMED_RATIO:-0}"
RESET_STATE="${RESET_STATE:-1}"
BASE_TIME="${BASE_TIME:-2026-05-01T12:00:00Z}"
DASHBOARD_TIME_FROM="${DASHBOARD_TIME_FROM:-2026-05-01T10:30:00Z}"
DASHBOARD_TIME_TO="${DASHBOARD_TIME_TO:-2026-05-01T12:05:00Z}"
ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-http://localhost:${ELASTICSEARCH_HOST_PORT:-19200}}"
KIBANA_URL="${KIBANA_URL:-http://localhost:${KIBANA_HOST_PORT:-15601}}"

wait_for_index_count() {
  local expected="$1"
  local attempt
  for attempt in $(seq 1 90); do
    local count
    count="$(curl -fsS "$ELASTICSEARCH_URL/product-events/_count" 2>/dev/null | jq -r '.count // 0' || true)"
    if [[ "$count" =~ ^[0-9]+$ ]] && (( count >= expected )); then
      echo "Indexed $count product events."
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for at least $expected indexed documents" >&2
  return 1
}

cd "$ROOT_DIR"

if [[ "$RESET_STATE" == "1" ]]; then
  "$ROOT_DIR/scripts/clean-demo-state.sh"
fi

"$ROOT_DIR/scripts/create-topics.sh"
"$ROOT_DIR/scripts/create-search-resources.sh"
"$ROOT_DIR/scripts/register-connectors.sh"
DASHBOARD_TIME_FROM="$DASHBOARD_TIME_FROM" DASHBOARD_TIME_TO="$DASHBOARD_TIME_TO" "$ROOT_DIR/scripts/create-kibana-dashboard.sh"

total_events=$((RATE_PER_SECOND * DURATION_SECONDS))
echo "Generating $total_events hot-product events across the last $DURATION_SECONDS seconds..."
echo "Using deterministic base time: $BASE_TIME"

GRADLE_DOCKER_NETWORK="$GRADLE_DOCKER_NETWORK" KAFKA_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" \
  "$ROOT_DIR/scripts/run-gradle.sh" --no-daemon run --args="generate --rate-per-second=$RATE_PER_SECOND --duration-seconds=$DURATION_SECONDS --initial-stock=$INITIAL_STOCK --seed=$SEED --base-time=$BASE_TIME --malformed-ratio=$MALFORMED_RATIO"

wait_for_index_count "$total_events"

curl -fsS "$ELASTICSEARCH_URL/product-events/_search?size=0" \
  -H "Content-Type: application/json" \
  -d "{\"query\":{\"range\":{\"occurred_at\":{\"gte\":\"$DASHBOARD_TIME_FROM\",\"lte\":\"$DASHBOARD_TIME_TO\"}}},\"aggs\":{\"types\":{\"terms\":{\"field\":\"event_type\",\"size\":10}}}}" \
  | jq -r '.aggregations.types.buckets[] | "\(.key)=\(.doc_count)"'

echo "Dashboard: $KIBANA_URL/app/dashboards#/view/hot-product-sales-dashboard"
