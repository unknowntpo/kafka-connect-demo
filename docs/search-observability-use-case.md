# Hot Product Search / Observability Demo Design

## Summary

This Kafka Connect demo uses a single hot-selling product to make the dashboard story obvious to any audience.

The user-facing question is simple:

```text
Is this product becoming hot, is stock running out, and why are purchases failing?
```

Architecture:

```text
Java Hot Product Event Generator
        |
        | JSON events
        v
Kafka topic: product.events
        |
        | Kafka Connect Elasticsearch Sink
        | Converter + SMT + DLQ
        v
Elasticsearch index: product-events
        |
        v
Kibana
```

Kafka Connect is responsible for moving events from Kafka into Elasticsearch. The Java app only simulates realistic product activity.

## Dashboard Story

The dashboard should show these trends:

- Product views per minute are increasing.
- Buy clicks increase after the product becomes hot.
- Successful purchases increase while stock is available.
- Remaining stock decreases over time.
- Failed purchases increase after the product sells out.
- Failure reasons show whether users hit `OUT_OF_STOCK`, `PAYMENT_FAILED`, or `RATE_LIMITED`.

Implemented Kibana panels:

- Metric: total indexed product events.
- Line chart: event volume over time, split by `event_type`.
- Table: `BUY_CLICKED`, `PURCHASE_SUCCEEDED`, and `PURCHASE_FAILED` counts.
- Table: `failure_reason`, highlighting `OUT_OF_STOCK`.
- Table: top generated `user_id` values by event count.

The dashboard is created by:

```bash
./scripts/create-kibana-dashboard.sh
```

Dashboard URL:

```text
http://localhost:5601/app/dashboards#/view/hot-product-sales-dashboard
```

## Event Model

Event types:

```text
PRODUCT_VIEWED
BUY_CLICKED
PURCHASE_SUCCEEDED
PURCHASE_FAILED
STOCK_CHANGED
```

Common fields:

```json
{
  "event_id": "evt_...",
  "event_type": "PRODUCT_VIEWED",
  "product_id": "sku_hot_001",
  "product_name": "Limited Edition Keyboard",
  "user_id": "user_0042",
  "session_id": "sess_abcd",
  "occurred_at": "2026-04-29T10:00:00Z",
  "service": "web",
  "severity": "INFO",
  "remaining_stock": 42,
  "message": "Hot product page viewed",
  "metadata": {
    "region": "ap-northeast-1",
    "campaign": "creator-drop"
  }
}
```

Purchase success adds:

```json
{
  "order_id": "order_9001",
  "price": 129.99
}
```

Purchase failure adds:

```json
{
  "failure_reason": "OUT_OF_STOCK"
}
```

## Event Generator

The generator is intentionally simple and stateful:

```text
remaining_stock = initial_stock

warmup:
  mostly PRODUCT_VIEWED
  some BUY_CLICKED

hot:
  more BUY_CLICKED
  more PURCHASE_SUCCEEDED
  stock decreases

sold_out:
  PURCHASE_SUCCEEDED stops
  PURCHASE_FAILED with OUT_OF_STOCK increases
```

CLI shape:

```bash
./scripts/run-gradle.sh --no-daemon run --args="generate --rate-per-second=30 --duration-seconds=20 --initial-stock=80 --seed=42"
```

For local demos, use a lower event rate if Kibana or Elasticsearch returns HTTP 429:

```bash
./scripts/run-gradle.sh --no-daemon run --args="generate --rate-per-second=10 --duration-seconds=20 --initial-stock=80 --seed=42"
```

For the dashboard story, seed a larger historical data set:

```bash
./scripts/seed-dashboard-data.sh
```

The default seed generates `14,400` events over the last hour. Timestamps are intentionally denser near the end of the window, so the dashboard shows a hot-product surge rather than a flat test-data line.

Important options:

- `--rate-per-second`
- `--duration-seconds`
- `--initial-stock`
- `--seed`
- `--no-seed`
- `--malformed-ratio`
- `--realtime`
- `--flat-traffic`

Seeded runs are used for E2E tests. Unseeded runs are useful for live demos.

## Chapter 3 Alignment

This demo shows Kafka Connect's pipeline components:

- `Runtime`: distributed Kafka Connect worker.
- `REST API`: connector creation and status inspection.
- `Plug-ins`: Elasticsearch sink installed through `plugin.path`.
- `Connector`: Elasticsearch sink connector.
- `Task`: sink task consumes Kafka partitions.
- `Converter`: `JsonConverter` converts Kafka bytes into `ConnectRecord`.
- `SMT`: `Flatten` promotes nested metadata such as `metadata.region` into dashboard-friendly fields such as `metadata_region`; `InsertField` adds `pipeline=connect-search-demo`.
- `DLQ`: malformed JSON records are routed to `product.events.dlq`.

Sink-side flow:

```text
Kafka bytes
    |
    | JsonConverter
    v
ConnectRecord
    |
    | SMT
    v
ConnectRecord
    |
    | Elasticsearch Sink Task
    v
Elasticsearch document
```

## Chapter 4 Alignment

This demo also shows pipeline design decisions:

- Search sink is chosen because the target workload is query, dashboarding, and event inspection.
- The event schema is intentionally small and dashboard-driven.
- `product.events` uses 3 partitions so sink task parallelism can be discussed.
- `tasks.max=2` shows that task count is bounded by partitions and connector behavior.
- SMT is used for light, record-local reshaping: flattening metadata for dashboard grouping and adding pipeline provenance. It is not used for business logic.
- Malformed records go to DLQ.
- Elasticsearch writes are treated as at-least-once.
- Kafka record key is `event_id`, letting the sink use stable document ids for practical idempotency.

## E2E Criteria

The implementation is complete when `./scripts/e2e.sh` verifies:

- Kafka, Connect, Elasticsearch, and Kibana start.
- Elasticsearch sink connector plugin is discoverable.
- `product.events` and `product.events.dlq` are created.
- Connector and task reach `RUNNING`.
- Hot product generator writes events to Kafka.
- Elasticsearch receives indexed documents.
- Kibana can use the `product-events` data view.
- Kibana dashboard saved objects can be created for the demo metrics.
- SMT field `pipeline=connect-search-demo` and SMT-flattened field `metadata_region` appear in indexed documents.
- Malformed records are written to DLQ.
- Connect can restart and continue indexing new events.

## Rerun Isolation

Demo seed scripts call `./scripts/clean-demo-state.sh` by default. The cleanup removes:

- the sink connector
- `product.events`
- `product.events.dlq`
- Kafka Connect internal topics
- the Elasticsearch `product-events` index

This keeps repeated demo runs deterministic instead of mixing new events with previous connector offsets, topic contents, or indexed documents.

The seed scripts also use deterministic event time by default:

```text
BASE_TIME=2026-05-01T12:00:00Z
```

The Kibana dashboard time range is set to the generated data window during seeding, so reruns show the same event counts, trend buckets, and region/failure distributions.
