#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-host.docker.internal:9092}"
GRADLE_DOCKER_NETWORK="${GRADLE_DOCKER_NETWORK:-}"

network_args=()
if [[ -n "$GRADLE_DOCKER_NETWORK" ]]; then
  network_args=(--network "$GRADLE_DOCKER_NETWORK")
fi

docker run --rm \
  "${network_args[@]}" \
  -u "$(id -u):$(id -g)" \
  -e KAFKA_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" \
  -e GRADLE_USER_HOME=/workspace/.gradle-cache \
  -v "$ROOT_DIR:/workspace" \
  -w /workspace/app \
  gradle:8.7-jdk21 \
  gradle "$@"
