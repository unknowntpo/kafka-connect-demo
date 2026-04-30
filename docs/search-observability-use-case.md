# 熱門商品搜尋與觀測 Demo 設計

## 摘要

這個 Kafka Connect demo 使用「單一熱門商品」與「限量折價券」兩種容易理解的情境，讓學生可以直接看懂資料管線的目的。

核心問題是：

```text
這個商品是否正在變熱門？
庫存或折價券是否即將售罄？
購買或領券失敗的原因是什麼？
```

架構：

```text
Java 事件產生器
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

Kafka Connect 負責把 Kafka 裡的事件送到 Elasticsearch。Java application 只負責模擬使用者行為與商業事件。

## Dashboard 故事

Dashboard 應該呈現下列趨勢：

- 商品瀏覽量逐步上升。
- 商品變熱後，購買點擊增加。
- 庫存存在時，成功購買或成功領券增加。
- 庫存或折價券數量逐步下降。
- 售罄後，失敗事件增加。
- failure reasons 顯示使用者遇到的是 `OUT_OF_STOCK`、`COUPON_SOLD_OUT`、`PAYMENT_FAILED` 或 `RATE_LIMITED`。

已實作的 Kibana panels：

- Metric：已索引事件總數。
- Line chart：依 `event_type` 切分的事件量趨勢。
- Table：business outcomes，例如點擊、成功、失敗。
- Table：`failure_reason`，用來觀察售罄或限流。
- Table：依 `user_id` 統計的活躍使用者。
- Table：依 `metadata_region` 統計的地區流量。`metadata_region` 由 Kafka Connect `Flatten` SMT 從巢狀 `metadata.region` 展平而來。

建立 dashboard：

```bash
./scripts/create-kibana-dashboard.sh
```

Dashboard URL：

```text
http://localhost:5601/app/dashboards#/view/hot-product-sales-dashboard
```

## 事件模型

熱門商品事件類型：

```text
PRODUCT_VIEWED
BUY_CLICKED
PURCHASE_SUCCEEDED
PURCHASE_FAILED
STOCK_CHANGED
```

限量折價券事件類型：

```text
COUPON_VIEWED
PAGE_REFRESHED
WAITING_ROOM_JOINED
COUPON_CLAIM_SUCCEEDED
COUPON_CLAIM_FAILED
```

共同欄位範例：

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

購買成功會增加：

```json
{
  "order_id": "order_9001",
  "price": 129.99
}
```

購買失敗會增加：

```json
{
  "failure_reason": "OUT_OF_STOCK"
}
```

折價券情境會增加：

```json
{
  "coupon_id": "coupon_mayday_001",
  "remaining_coupons": 0,
  "failure_reason": "COUPON_SOLD_OUT"
}
```

## 事件產生器

熱門商品 generator 是 stateful 的簡化模型：

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

CLI 範例：

```bash
./scripts/run-gradle.sh --no-daemon run --args="generate --rate-per-second=30 --duration-seconds=20 --initial-stock=80 --seed=42"
```

如果本機 Kibana 或 Elasticsearch 回傳 HTTP 429，可以降低 event rate：

```bash
./scripts/run-gradle.sh --no-daemon run --args="generate --rate-per-second=10 --duration-seconds=20 --initial-stock=80 --seed=42"
```

產生較大的 dashboard 資料集：

```bash
./scripts/seed-dashboard-data.sh
```

預設會產生 `14,400` 筆事件，並讓時間戳越接近後段越密集，使 dashboard 呈現商品爆紅的趨勢。

重要參數：

- `--rate-per-second`
- `--duration-seconds`
- `--initial-stock`
- `--seed`
- `--no-seed`
- `--malformed-ratio`
- `--realtime`
- `--flat-traffic`
- `--profile`
- `--base-time`

E2E 測試使用 seeded runs；live demo 可以改用 unseeded runs。

## 第三章對應

這個 demo 展示 Kafka Connect pipeline components：

- `Runtime`：distributed Kafka Connect worker。
- `REST API`：connector creation 與 status inspection。
- `Plug-ins`：Elasticsearch sink 透過 `plugin.path` 載入。
- `Connector`：Elasticsearch sink connector。
- `Task`：sink task 從 Kafka partitions 消費資料。
- `Converter`：`JsonConverter` 把 Kafka bytes 轉成 `ConnectRecord`。
- `SMT`：`Flatten` 將 `metadata.region` 展平成 `metadata_region`；`InsertField` 加上 `pipeline=connect-search-demo`。
- `DLQ`：malformed JSON records 會被送到 `product.events.dlq`。

Sink-side flow：

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

## 第四章對應

這個 demo 展示下列 pipeline design decisions：

- 選擇 search sink，因為目標工作負載是查詢、dashboarding 與事件檢索。
- event schema 依 dashboard 問題設計，而不是任意塞欄位。
- `product.events` 使用 3 partitions，便於討論 sink task parallelism。
- `tasks.max=2` 顯示 task 數量受 partitions、connector 行為與外部系統限制。
- SMT 只做 record-local reshaping：展平 metadata 供 dashboard 分組，以及加入 pipeline provenance；不承擔 business logic。
- malformed records 進入 DLQ。
- Elasticsearch writes 以 at-least-once 理解。
- Kafka record key 使用 `event_id`，讓 sink 可以使用穩定 document id 實作 practical idempotency。

## E2E 驗證條件

`./scripts/e2e.sh` 會驗證：

- Kafka、Connect、Elasticsearch、Kibana 啟動。
- Elasticsearch sink connector plug-in 可被發現。
- `product.events` 與 `product.events.dlq` 建立完成。
- connector 與 task 進入 `RUNNING`。
- event generator 寫入 Kafka。
- Elasticsearch 收到 indexed documents。
- Kibana 可以使用 `product-events` data view。
- Kibana dashboard saved objects 可以建立。
- SMT 欄位 `pipeline=connect-search-demo` 與 `metadata_region` 出現在 indexed documents。
- malformed records 寫入 DLQ。
- Connect restart 後仍可繼續 indexing。

## 重跑隔離

demo seed scripts 預設會呼叫：

```bash
./scripts/clean-demo-state.sh
```

cleanup 會移除：

- sink connector
- `product.events`
- `product.events.dlq`
- Kafka Connect internal topics
- Elasticsearch `product-events` index

這樣重跑 demo 時，不會混入前一次執行留下的 connector offsets、topic contents 或 indexed documents。

seed scripts 也預設使用 deterministic event time：

```text
BASE_TIME=2026-05-01T12:00:00Z
```

Kibana dashboard time range 會在 seeding 時設定到產生資料的時間窗，因此每次重跑都會得到相同的 event counts、trend buckets 與 region/failure distributions。
