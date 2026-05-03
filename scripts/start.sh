#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVICES=("$@")

check_port_available() {
  local name="$1"
  local port="$2"
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "Host port $port for $name is already in use." >&2
    echo "Override it with ${name}_HOST_PORT=<free-port>." >&2
    return 1
  fi
}

needs_service() {
  local service="$1"
  if (( ${#SERVICES[@]} == 0 )); then
    return 0
  fi

  local requested
  for requested in "${SERVICES[@]}"; do
    if [[ "$requested" == "$service" ]]; then
      return 0
    fi
  done
  return 1
}

cd "$ROOT_DIR"
mkdir -p sink

if [[ -z "$(docker compose ps --status running -q)" ]]; then
  if needs_service broker; then
    check_port_available KAFKA "${KAFKA_HOST_PORT:-19092}"
  fi
  if needs_service connect; then
    check_port_available CONNECT "${CONNECT_HOST_PORT:-18083}"
  fi
  if needs_service redpanda-console; then
    check_port_available REDPANDA_CONSOLE "${REDPANDA_CONSOLE_HOST_PORT:-18080}"
  fi
  if needs_service elasticsearch; then
    check_port_available ELASTICSEARCH "${ELASTICSEARCH_HOST_PORT:-19200}"
  fi
  if needs_service kibana; then
    check_port_available KIBANA "${KIBANA_HOST_PORT:-15601}"
  fi
fi

docker compose up -d --build "${SERVICES[@]}"
