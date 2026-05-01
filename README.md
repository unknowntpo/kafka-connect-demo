# Kafka Connect 熱門商品搜尋與觀測 Demo

這個專案示範一條簡化但完整的 Kafka Connect 觀測管線：把熱門商品或限量折價券的事件送進 Kafka，再透過 Kafka Connect 寫入 Elasticsearch，最後在 Kibana dashboard 觀察趨勢。

```text
Java 事件產生器
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
Kibana dashboard / search UI
```

故事刻意設計得容易理解：許多使用者瀏覽商品、點擊購買或搶折價券；部分操作成功，庫存逐漸下降；售罄後失敗事件上升。學生可以直接在 Kibana 上看到資料流動與事件趨勢。

## 給 Mentor 的設計說明

這個 demo 的教學方式是「先看見結果，再回到管線設計」。學生先在 Kibana dashboard 看到熱門商品或限量折價券的流量變化、成功與失敗、售罄原因，再回頭拆解事件如何經過 Kafka、Kafka Connect、SMT、DLQ，最後寫入 Elasticsearch。

Kafka Connect 在這裡不是為了取代一個很短的 Java consumer，而是用來示範實務資料管線的責任邊界：應用程式負責產生業務事件，Kafka 負責承接事件流，Kafka Connect 負責把 Kafka records 穩定寫到外部系統，並提供 connector lifecycle、task 狀態、converter、SMT、DLQ 與 restart 行為。這些是第三章與第四章要討論的核心。

Elasticsearch 的選擇是因為本 demo 的目標是搜尋與觀測，而不是交易系統或長期資料倉儲。它能直接接收 JSON 事件、依時間與欄位查詢、做聚合，並透過 Kibana 讓學生看到 dashboard。OpenSearch、Splunk、ClickHouse、Loki 都可能出現在相似場景，但本 demo 優先選擇 Elasticsearch，是因為它和 Kibana 的整合最直覺，且 Kafka Connect Elasticsearch sink 能清楚展示「Kafka 到外部搜尋/觀測系統」的 sink pipeline。

## 這個 Demo 展示什麼

Chapter 3 相關概念：

- Kafka Connect distributed runtime
- 透過 `/connector-plugins` 檢查 plug-in
- 透過 REST API 管理 connector lifecycle
- connector 與 task 狀態
- sink-side converter flow
- `Flatten` 與 `InsertField` SMT
- malformed records 的 DLQ
- Kafka Connect internal topics

Chapter 4 相關概念：

- 如何為搜尋與觀測情境選擇 connector
- 如何根據 dashboard 需求設計 event model
- partitions 與 `tasks.max` 的關係
- sink failure visibility
- at-least-once sink behavior
- 使用 Kafka record key 作為 Elasticsearch document id，達成實務上的 idempotency

## 服務與網址

- Kafka broker: `localhost:9092`
- Kafka Connect REST: `localhost:8083`
- Redpanda Console: `http://localhost:8080`
- Elasticsearch: `http://localhost:9200`
- Kibana: `http://localhost:5601`

## 快速開始

```bash
./scripts/start.sh
./scripts/wait-for-connect.sh
./scripts/create-topics.sh
./scripts/create-search-resources.sh
./scripts/register-connectors.sh
./scripts/inspect-status.sh
```

產生一小批熱門商品事件：

```bash
./scripts/run-gradle.sh --no-daemon run --args="generate --rate-per-second=10 --duration-seconds=20 --initial-stock=80 --seed=42"
```

產生較真實的熱門商品 dashboard 資料：

```bash
./scripts/seed-dashboard-data.sh
```

預設會產生 `14,400` 筆事件，並讓事件越接近時間窗後段越密集，以模擬商品變熱的趨勢。

產生 AI profile-driven 的限量折價券搶購資料：

```bash
./scripts/seed-ai-load-profile.sh
```

這個腳本會使用 [profiles/flash-sale-coupon.json](profiles/flash-sale-coupon.json) 產生 `24,000` 筆事件，並透過 [scripts/score-load-profile.sh](scripts/score-load-profile.sh) 查詢 Elasticsearch 進行評分。設計說明在 [docs/ai-powered-load-generator.md](docs/ai-powered-load-generator.md)。

seed 腳本預設是隔離且可重跑的。產生資料前會清除 connector、Kafka data topics、Kafka Connect internal topics 與 Elasticsearch index，避免繼承上一次執行的狀態。腳本也使用固定的預設 base time：`2026-05-01T12:00:00Z`，因此每次重跑會得到相同的 dashboard 時間窗與聚合結果。

查看 Kafka topic：

```bash
./scripts/inspect-topic.sh product.events 5
```

查看 Elasticsearch：

```bash
./scripts/verify-sink.sh
```

建立 Kibana dashboard：

```bash
./scripts/create-kibana-dashboard.sh
```

打開 dashboard：

```text
http://localhost:5601/app/dashboards#/view/hot-product-sales-dashboard
```

## 事件類型

- `PRODUCT_VIEWED`
- `BUY_CLICKED`
- `PURCHASE_SUCCEEDED`
- `PURCHASE_FAILED`
- `STOCK_CHANGED`
- `COUPON_VIEWED`
- `PAGE_REFRESHED`
- `WAITING_ROOM_JOINED`
- `COUPON_CLAIM_SUCCEEDED`
- `COUPON_CLAIM_FAILED`

範例事件：

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

## Dashboard 會看到什麼

內建 Kibana dashboard 包含：

- 已索引事件總數
- 依 `event_type` 切分的事件量趨勢
- business outcomes：點擊、成功、失敗
- failure reasons，特別是 `OUT_OF_STOCK` 或 `COUPON_SOLD_OUT`
- 活躍使用者分布
- 透過 `Flatten` SMT 產生的 `metadata_region`，展示不同地區的流量分布

## E2E 驗證

執行：

```bash
./scripts/e2e.sh
```

E2E 會驗證：

- stack startup
- connector plug-in discovery
- 建立 3 partitions 的 `product.events`
- 透過 REST API 註冊 connector
- connector 與 task 進入 `RUNNING`
- event generator 寫入 Kafka
- Elasticsearch 收到 indexed documents
- SMT 欄位 `pipeline=connect-search-demo`
- SMT 展平後的 `metadata_region`
- malformed records 被送到 `product.events.dlq`
- Connect restart 後仍可繼續 indexing
- Kafka Connect internal topics 已建立

Dashboard setup script 可以重跑；它會覆蓋相同 saved object ids。dashboard 預設 refresh interval 是 30 秒，避免本機 Elasticsearch container 壓力過大。seed 腳本會覆蓋 dashboard time range，使它對齊固定產生的事件時間窗。

## 重置

完整關閉並清掉 Docker volumes：

```bash
./scripts/reset.sh
```

如果 stack 已經在執行，只想做一次乾淨重跑：

```bash
./scripts/clean-demo-state.sh
```
