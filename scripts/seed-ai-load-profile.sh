#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${PROFILE:-profiles/flash-sale-coupon.json}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-broker:29092}"
GRADLE_DOCKER_NETWORK="${GRADLE_DOCKER_NETWORK:-kafka-connect-demo_default}"
SEED="${SEED:-20260430}"
MALFORMED_RATIO="${MALFORMED_RATIO:-0}"
RESET_STATE="${RESET_STATE:-1}"
BASE_TIME="${BASE_TIME:-2026-05-01T12:00:00Z}"
DASHBOARD_TIME_FROM="${DASHBOARD_TIME_FROM:-2026-05-01T10:30:00Z}"
DASHBOARD_TIME_TO="${DASHBOARD_TIME_TO:-2026-05-01T12:05:00Z}"
FIRST_FROM="${FIRST_FROM:-2026-05-01T10:30:00Z}"
FIRST_TO="${FIRST_TO:-2026-05-01T11:00:00Z}"
MIDDLE_FROM="${MIDDLE_FROM:-2026-05-01T11:00:00Z}"
MIDDLE_TO="${MIDDLE_TO:-2026-05-01T11:30:00Z}"
LAST_FROM="${LAST_FROM:-2026-05-01T11:30:00Z}"
LAST_TO="${LAST_TO:-2026-05-01T12:05:00Z}"
ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-http://localhost:${ELASTICSEARCH_HOST_PORT:-19200}}"
KIBANA_URL="${KIBANA_URL:-http://localhost:${KIBANA_HOST_PORT:-15601}}"

cd "$ROOT_DIR"

if [[ ! -f "$PROFILE" ]]; then
  echo "Profile not found: $PROFILE" >&2
  exit 1
fi

expected_events="$(jq -r '.total_events' "$PROFILE")"

if [[ "$RESET_STATE" == "1" ]]; then
  "$ROOT_DIR/scripts/clean-demo-state.sh"
fi

"$ROOT_DIR/scripts/create-topics.sh"
"$ROOT_DIR/scripts/create-search-resources.sh"
"$ROOT_DIR/scripts/register-connectors.sh"
DASHBOARD_TIME_FROM="$DASHBOARD_TIME_FROM" DASHBOARD_TIME_TO="$DASHBOARD_TIME_TO" "$ROOT_DIR/scripts/create-kibana-dashboard.sh"

echo "Generating $expected_events events from AI load profile: $PROFILE"
echo "Using deterministic base time: $BASE_TIME"

GRADLE_DOCKER_NETWORK="$GRADLE_DOCKER_NETWORK" KAFKA_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" \
  "$ROOT_DIR/scripts/run-gradle.sh" --no-daemon run --args="generate --profile=/workspace/$PROFILE --seed=$SEED --base-time=$BASE_TIME --malformed-ratio=$MALFORMED_RATIO"

for attempt in $(seq 1 90); do
  count="$(curl -fsS "$ELASTICSEARCH_URL/product-events/_count" 2>/dev/null | jq -r '.count // 0' || true)"
  if [[ "$count" =~ ^[0-9]+$ ]] && (( count >= expected_events )); then
    echo "Indexed $count events from AI load profile."
    WINDOW_FROM="$DASHBOARD_TIME_FROM" WINDOW_TO="$DASHBOARD_TIME_TO" \
      FIRST_FROM="$FIRST_FROM" FIRST_TO="$FIRST_TO" \
      MIDDLE_FROM="$MIDDLE_FROM" MIDDLE_TO="$MIDDLE_TO" \
      LAST_FROM="$LAST_FROM" LAST_TO="$LAST_TO" \
      "$ROOT_DIR/scripts/score-load-profile.sh"
    echo "Dashboard: $KIBANA_URL/app/dashboards#/view/hot-product-sales-dashboard"
    exit 0
  fi
  sleep 2
done

echo "Timed out waiting for $expected_events indexed events" >&2
exit 1
