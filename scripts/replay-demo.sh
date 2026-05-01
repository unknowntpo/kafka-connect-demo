#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

"$ROOT_DIR/scripts/start.sh"
"$ROOT_DIR/scripts/wait-for-connect.sh"
RESET_STATE=1 "$ROOT_DIR/scripts/seed-ai-load-profile.sh"

echo "Replay demo completed."
echo "Dashboard: http://localhost:5601/app/dashboards#/view/hot-product-sales-dashboard"
