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

Kafka Connect 在這裡用來示範實務資料管線的責任邊界：應用程式負責產生業務事件，Kafka 負責承接事件流，Kafka Connect 負責把 Kafka records 穩定寫到外部系統，並提供 connector lifecycle、task 狀態、converter、SMT、DLQ 與 restart 行為。這些是第三章與第四章要討論的核心。

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

## Dashboard As Code

Kibana dashboard 定義放在 [dashboards/hot-product-sales-observability.ndjson](/Users/unknowntpo/repo/unknowntpo/kafka-connect-demo/dashboards/hot-product-sales-observability.ndjson)。

這個檔案是 Kibana Saved Objects NDJSON，包含：

- data view：`product-events`
- 6 個 visualization panels
- dashboard layout 與 references

建立或覆蓋 dashboard：

```bash
just dashboard
```

`just run-demo` 也會自動匯入這份 dashboard 定義，並把 dashboard time range 設定到固定 demo 時間窗。

## 投影片

Slidev 投影片在 [docs/chapter-4-intro-slides.md](/Users/unknowntpo/repo/unknowntpo/kafka-connect-demo/docs/chapter-4-intro-slides.md)。

建議呈現順序：

```text
問題場景 -> 追蹤指標 -> Database / Elasticsearch 取捨 -> Kafka -> Kafka Connect -> Demo -> 第四章 component breakdown
```

安裝 Slidev 依賴：

```bash
npm install
```

本專案使用 `Justfile` 作為主要操作入口。可用指令可透過下列命令查看：

```bash
just
```

播放投影片：

```bash
just slides
```

驗證投影片可以 build：

```bash
just slides-build
```

## 快速開始

正式展示或課堂 demo 建議使用單一重播入口。此腳本會啟動 Docker Compose stack、清理上一輪狀態、重新建立 connector/topic/index/dashboard，並從執行當下開始產生同一批劇本資料：

```bash
just run-demo
```

Dashboard：

```text
http://localhost:5601/app/dashboards#/view/hot-product-sales-dashboard
```

如果需要逐步觀察各元件啟動流程，可以改用下列指令：

```bash
just setup
```

產生一小批熱門商品事件：

```bash
just run-basic
```

產生較真實的熱門商品 dashboard 資料：

```bash
just seed-dashboard
```

預設會產生 `14,400` 筆事件，並讓事件越接近時間窗後段越密集，以模擬商品變熱的趨勢。

產生 AI profile-driven 的限量折價券搶購資料：

```bash
just seed-ai
```

這個腳本會使用 [profiles/flash-sale-coupon.json](profiles/flash-sale-coupon.json) 產生 `24,000` 筆事件，並透過 [scripts/score-load-profile.sh](scripts/score-load-profile.sh) 查詢 Elasticsearch 進行評分。profile 會先為每個 participant 產生一筆 `COUPON_VIEWED`，再產生重新整理、等候室與領券結果等後續行為。設計說明在 [docs/ai-powered-load-generator.md](docs/ai-powered-load-generator.md)。

seed 腳本預設是隔離且可重跑的。產生資料前會清除 connector、Kafka data topics、Kafka Connect internal topics 與 Elasticsearch index，避免繼承上一次執行的狀態。事件預設會從執行當下開始產生，dashboard time range 也會自動對齊這次產生的事件時間窗；若需要固定時間重現，可手動設定 `BASE_TIME` 或 `EVENT_START_TIME`。

查看 Kafka topic：

```bash
just inspect-topic product.events 5
```

查看 Elasticsearch：

```bash
just verify-sink
```

建立 Kibana dashboard：

```bash
just dashboard
```

打開 dashboard：

```text
http://localhost:5601/app/dashboards#/view/hot-product-sales-dashboard
```

## 主要事件類型

限量折價券搶領情境主要使用下列事件：

- `COUPON_VIEWED`
- `PAGE_REFRESHED`
- `WAITING_ROOM_JOINED`
- `COUPON_CLAIM_SUCCEEDED`
- `COUPON_CLAIM_FAILED`

範例事件：

```json
{
  "event_id": "evt_...",
  "event_type": "COUPON_CLAIM_FAILED",
  "product_id": "sku_hot_001",
  "coupon_id": "coupon_may_sale",
  "user_id": "user_0042",
  "session_id": "sess_abcd",
  "occurred_at": "run_started_at + 15s",
  "service": "coupon-service",
  "severity": "WARN",
  "remaining_coupons": 0,
  "failure_reason": "COUPON_SOLD_OUT",
  "metadata": {
    "region": "TW-NORTH",
    "campaign": "may-sale"
  }
}
```

## Dashboard 會看到什麼

內建 Kibana dashboard 包含：

- 已索引事件總數
- 依 `event_type` 切分的事件量趨勢
- 熱門商品行為統計：五種事件類型的事件數，總計應與事件總數一致。
- failure reasons，特別是 `OUT_OF_STOCK` 或 `COUPON_SOLD_OUT`
- 高頻操作線索，用來觀察短時間內反覆重新整理或多次領券失敗是否集中於少數使用者；這是排查線索，不代表 bot 偵測。
- 透過 `Flatten` SMT 產生的 `metadata_region`，展示不同地區的流量分布

## E2E 驗證

執行：

```bash
just e2e
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
just reset
```

如果 stack 已經在執行，只想做一次乾淨重跑：

```bash
just clean
```
