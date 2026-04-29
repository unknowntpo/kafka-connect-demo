#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${PROFILE:-profiles/flash-sale-coupon.json}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-broker:29092}"
GRADLE_DOCKER_NETWORK="${GRADLE_DOCKER_NETWORK:-kafka-connect-demo_default}"
SEED="${SEED:-20260430}"
MALFORMED_RATIO="${MALFORMED_RATIO:-0}"
RESET_INDEX="${RESET_INDEX:-1}"

cd "$ROOT_DIR"

if [[ ! -f "$PROFILE" ]]; then
  echo "Profile not found: $PROFILE" >&2
  exit 1
fi

expected_events="$(jq -r '.total_events' "$PROFILE")"

"$ROOT_DIR/scripts/create-topics.sh"

if [[ "$RESET_INDEX" == "1" ]]; then
  curl -fsS -X DELETE http://localhost:9200/product-events >/dev/null 2>&1 || true
fi

"$ROOT_DIR/scripts/create-search-resources.sh"
"$ROOT_DIR/scripts/register-connectors.sh"
"$ROOT_DIR/scripts/create-kibana-dashboard.sh"

echo "Generating $expected_events events from AI load profile: $PROFILE"

GRADLE_DOCKER_NETWORK="$GRADLE_DOCKER_NETWORK" KAFKA_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" \
  "$ROOT_DIR/scripts/run-gradle.sh" --no-daemon run --args="generate --profile=/workspace/$PROFILE --seed=$SEED --malformed-ratio=$MALFORMED_RATIO"

for attempt in $(seq 1 90); do
  count="$(curl -fsS 'http://localhost:9200/product-events/_count' 2>/dev/null | jq -r '.count // 0' || true)"
  if [[ "$count" =~ ^[0-9]+$ ]] && (( count >= expected_events )); then
    echo "Indexed $count events from AI load profile."
    "$ROOT_DIR/scripts/score-load-profile.sh"
    echo "Dashboard: http://localhost:5601/app/dashboards#/view/hot-product-sales-dashboard"
    exit 0
  fi
  sleep 2
done

echo "Timed out waiting for $expected_events indexed events" >&2
exit 1
