set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
    just --list

start:
    ./scripts/start.sh

wait:
    ./scripts/wait-for-connect.sh

topics:
    ./scripts/create-topics.sh

search-resources:
    ./scripts/create-search-resources.sh

connectors:
    ./scripts/register-connectors.sh

dashboard:
    ./scripts/create-kibana-dashboard.sh

clean:
    ./scripts/clean-demo-state.sh

reset:
    ./scripts/reset.sh

seed-dashboard:
    ./scripts/seed-dashboard-data.sh

seed-ai:
    ./scripts/seed-ai-load-profile.sh

run-basic:
    ./scripts/run-gradle.sh --no-daemon run --args="generate --rate-per-second=10 --duration-seconds=20 --initial-stock=80 --seed=42"

inspect-topic topic="product.events" count="5":
    ./scripts/inspect-topic.sh {{topic}} {{count}}

verify-sink:
    ./scripts/verify-sink.sh

run-demo:
    ./scripts/replay-demo.sh

replay-demo:
    just run-demo

setup: start wait topics search-resources connectors dashboard status

e2e:
    ./scripts/e2e.sh

status:
    ./scripts/inspect-status.sh
