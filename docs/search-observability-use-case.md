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

## 給 Mentor 的教學設計

這份 demo 採用「情境優先」的教學方式。主軸不是先解釋所有 Kafka Connect 名詞，而是先設定一個學生容易理解的業務場景：熱門商品或限量折價券突然爆量，營運團隊需要知道流量是否升高、成功與失敗的比例、失敗原因，以及哪些地區或使用者最活躍。

教學順序如下：

1. 先看 Kibana dashboard，建立「資料流動後能回答什麼問題」的直覺。
2. 再回到事件模型，說明為什麼 event 必須包含 `event_type`、`occurred_at`、`user_id`、`failure_reason`、`metadata.region` 等欄位。
3. 接著拆解 Kafka Connect sink pipeline，說明 converter、SMT、task、DLQ、internal topics 各自處理哪一段責任。
4. 最後用 E2E 腳本驗證整條管線，而不是只展示 dashboard 截圖。

這樣安排的目的，是避免學生把 Kafka Connect 理解成「某個會搬資料的黑盒子」。demo 會讓學生看到：資料模型、connector 選型、SMT 使用方式、錯誤資料處理、重跑隔離與 dashboard 設計，都是同一條 pipeline design 的一部分。

## 為什麼需要 Kafka Connect

在這個 demo 中，Kafka Connect 做了下列事情：

- 從 Kafka topic `product.events` 持續讀取事件。
- 使用 `JsonConverter` 將 Kafka bytes 轉成 Kafka Connect record。
- 使用 SMT 將 `metadata.region` 展平成 `metadata_region`，並加上 `pipeline=connect-search-demo`。
- 將正常事件寫入 Elasticsearch index `product-events`。
- 將 malformed JSON 或無法處理的 record 寫入 `product.events.dlq`。
- 透過 REST API 暴露 connector 與 task 狀態，讓 demo 可以檢查 pipeline 是否健康。
- 使用 Kafka Connect internal topics 保存 connector config、offsets 與 status。

如果只為了 demo 最小路徑，確實可以寫一個 Java consumer 直接讀 Kafka 再呼叫 Elasticsearch API。此處選擇 Kafka Connect，是因為教學目標不是「寫一支可以跑的 consumer」，而是示範實務資料平台常見的 sink pipeline 設計。

Kafka Connect 的必要性可整理為：

| 面向 | 自寫 consumer | Kafka Connect |
| --- | --- | --- |
| 目標 | 快速完成單一需求 | 標準化 Kafka 與外部系統整合 |
| 部署與管理 | application 自行處理 | REST API 管理 connector lifecycle |
| 平行化 | application 自行設計 | connector task model |
| 狀態 | application 自行保存 offset 與健康狀態 | Connect internal topics 保存 config、offsets、status |
| 錯誤資料 | application 自行設計錯誤佇列 | DLQ 是 connector pipeline 的標準設計點 |
| 輕量轉換 | application code | SMT config |
| 教學價值 | 容易變成 consumer coding demo | 能對應 Kafka Connect 第三章與第四章 |

因此，這個 demo 對 Kafka Connect 的說法應保持精確：Kafka Connect 不是所有情境的必要條件；若資料量很小、管線只有一條、也沒有 connector lifecycle 與 DLQ 需求，自寫 consumer 可能更簡單。但當需求是穩定地把 Kafka 資料送到外部系統，並希望用標準方式管理設定、狀態、錯誤資料與重啟行為時，Kafka Connect 才開始展現價值。

## 為什麼選 Elasticsearch

本 demo 的目標是「Kafka -> 搜尋與觀測系統」，因此 sink target 需要符合下列條件：

- 能接收大量 JSON event。
- 能依時間欄位查詢事件趨勢。
- 能依 `event_type`、`failure_reason`、`metadata_region` 做聚合。
- 能快速搜尋單筆或一組事件。
- 能提供 dashboard，讓學生看到資料流動後的結果。

Elasticsearch 搭配 Kibana 符合這些需求。Elastic 官方文件也將 observability 描述為整合 logs、metrics、traces 等資料並在 Kibana 中視覺化與告警的方案；Elastic 的產品定位也包含 search、observability 與 security。對教學而言，Elasticsearch 的優點是 feedback loop 短：學生產生事件後，可以在 Kibana 立即看到總量、趨勢、失敗原因與地區分布。

此處不選關聯式資料庫作為主展示目標，是因為本 demo 不需要交易一致性、join-heavy reporting 或複雜 relational model。事件搜尋與觀測更需要的是時間序列查詢、欄位聚合、文字搜尋與 dashboard。

## 與相近產品的取捨

| 產品 | 適合場景 | 本 demo 的取捨 |
| --- | --- | --- |
| Elasticsearch + Kibana | 搜尋、log/event indexing、observability dashboard | 本 demo 採用；學生能直接看到 Kafka events 被查詢與視覺化 |
| OpenSearch + OpenSearch Dashboards | 類 Elasticsearch 的搜尋與分析，常見於 AWS 生態系 | 技術上可替代；若課程重點是 AWS managed service 或開源授權，可改用 OpenSearch |
| Splunk | 大型企業 log analytics、security、observability | 功能完整，但商業產品與部署成本較高，不適合做輕量 Docker Compose 教學主線 |
| ClickHouse / Druid / Pinot | 高吞吐 OLAP、即時分析、聚合查詢 | 適合分析型工作負載；但對初學者展示「事件搜尋 + Kibana dashboard」不如 Elasticsearch 直覺 |
| Grafana Loki | logs-first、低成本 log aggregation、Grafana 生態系 | 適合 logs pipeline；但對任意 JSON event 的欄位搜尋與聚合展示不如 Elasticsearch 直接 |

結論是：Elasticsearch 不是唯一正確答案，而是最符合本 demo 教學目標的答案。它能同時展示搜尋、聚合、dashboard 與 Kafka Connect sink pipeline。

## 大型公司會選 Elasticsearch 嗎

會，但不應把它描述成唯一選擇。Elastic 投資人資訊指出，Elastic 的 search、observability、security 解決方案被數千家公司使用，且包含超過半數 Fortune 500 公司。這能支持「Elasticsearch / Elastic Stack 是大型組織常見選項」這個說法。

但實務上，大型公司在相同 use case 下也可能選擇 OpenSearch、Splunk、ClickHouse、Datadog、Grafana Loki 或雲端供應商的原生服務。選型通常取決於既有雲端平台、授權政策、資安要求、查詢型態、資料量、保留天數、團隊維運能力與成本模型。

對學生的精確說法可以是：

```text
這個 demo 選 Elasticsearch，是因為它能把 Kafka event 變成可搜尋、可聚合、可視覺化的觀測資料。
在實務上，大型公司確實常使用 Elasticsearch / Elastic Stack 處理搜尋與觀測場景；
但同類場景也可能選 OpenSearch、Splunk 或其他分析平台。
工具選擇應回到資料型態、查詢需求、成本與維運能力。
```

參考資料：

- [Elastic Observability solution overview](https://www.elastic.co/guide/en/kibana/current/observability.html)
- [Elastic Investor Relations - Corporate Overview](https://ir.elastic.co/overview/default.aspx)
- [Amazon OpenSearch Service documentation](https://docs.aws.amazon.com/opensearch-service/latest/developerguide/what-is.html)
- [Splunk Observability](https://www.splunk.com/en_us/products/observability.html)

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
