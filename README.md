# Kafka Connect Hot Product Search Demo

This demo shows a simple observability pipeline for a hot-selling product:

```text
Java Hot Product Event Generator
        |
        | JSON events
        v
Kafka topic: product.events
        |
        | Kafka Connect Elasticsearch Sink
        | JsonConverter + SMT + DLQ
        v
Elasticsearch index: product-events
        |
        v
Kibana dashboard/search UI
```

The story is intentionally small: many users view one product, click buy, some purchases succeed, stock drops, and failed purchases rise after the product sells out.

## What It Demonstrates

Chapter 3 concepts:

- Kafka Connect distributed runtime
- plugin discovery through `/connector-plugins`
- connector lifecycle through REST
- connector and task status
- sink-side converter flow
- SMT with `InsertField`
- DLQ for malformed records
- Connect internal topics

Chapter 4 concepts:

- connector selection for search/observability
- event model design for dashboard queries
- partitions and `tasks.max`
- sink failure visibility
- at-least-once sink behavior
- practical idempotency using Kafka record key as Elasticsearch document id

## Services

- Kafka broker: `localhost:9092`
- Kafka Connect REST: `localhost:8083`
- Redpanda Console: `http://localhost:8080`
- Elasticsearch: `http://localhost:9200`
- Kibana: `http://localhost:5601`

## Quick Start

```bash
./scripts/start.sh
./scripts/wait-for-connect.sh
./scripts/create-topics.sh
./scripts/create-search-resources.sh
./scripts/register-connectors.sh
./scripts/inspect-status.sh
```

Generate hot product events:

```bash
./scripts/run-gradle.sh --no-daemon run --args="generate --rate-per-second=10 --duration-seconds=20 --initial-stock=80 --seed=42"
```

For a more realistic dashboard, seed one hour of historical hot-sale traffic:

```bash
./scripts/seed-dashboard-data.sh
```

By default this generates `14,400` events across the last hour, with denser traffic near the end of the window.

Inspect Kafka:

```bash
./scripts/inspect-topic.sh product.events 5
```

Inspect Elasticsearch:

```bash
./scripts/verify-sink.sh
```

Create the Kibana dashboard:

```bash
./scripts/create-kibana-dashboard.sh
```

Open:

```text
http://localhost:5601/app/dashboards#/view/hot-product-sales-dashboard
```

## Event Types

- `PRODUCT_VIEWED`
- `BUY_CLICKED`
- `PURCHASE_SUCCEEDED`
- `PURCHASE_FAILED`
- `STOCK_CHANGED`

Example event:

```json
{
  "event_id": "evt_...",
  "event_type": "PURCHASE_SUCCEEDED",
  "product_id": "sku_hot_001",
  "product_name": "Limited Edition Keyboard",
  "user_id": "user_0042",
  "session_id": "sess_abcd",
  "occurred_at": "2026-04-29T10:00:04Z",
  "service": "checkout",
  "severity": "INFO",
  "remaining_stock": 42,
  "order_id": "order_9001",
  "price": 129.99,
  "metadata": {
    "region": "ap-northeast-1",
    "campaign": "creator-drop"
  }
}
```

## Dashboard Targets

The included Kibana dashboard shows:

- Total indexed events
- Event volume over time, split by `event_type`
- Purchase outcomes: buy clicks, succeeded purchases, failed purchases
- Failure reasons, especially `OUT_OF_STOCK`
- Top active generated users

## E2E Verification

Run:

```bash
./scripts/e2e.sh
```

The E2E test verifies:

- stack startup
- connector plugin discovery
- topic creation with 3 partitions
- connector registration through REST
- connector and task `RUNNING`
- event generation into Kafka
- documents indexed into Elasticsearch
- SMT field `pipeline=connect-search-demo`
- malformed records routed to `product.events.dlq`
- Connect restart and continued indexing
- internal topic creation

The dashboard setup script can be rerun safely; it overwrites the same saved object ids.

The dashboard default time range is 90 minutes and the refresh interval is 30 seconds to avoid overloading a small local Elasticsearch container during demos.

## Reset

```bash
./scripts/reset.sh
```
