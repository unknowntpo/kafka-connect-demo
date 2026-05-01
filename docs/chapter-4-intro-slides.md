---
theme: default
title: Kafka Connect 第四章 - 用熱門商品 Demo 設計資料管線
info: |
  這份 Slidev 投影片用電商熱門商品觀測情境，介紹 Kafka、Kafka Connect、Elasticsearch 與 Kibana 如何組成一條可觀察的資料管線。
class: text-left
drawings:
  persist: false
transition: slide-left
mdc: true
---

# Kafka Connect 第四章

用熱門商品 Demo 理解資料管線設計

```text
Kafka -> Kafka Connect -> Elasticsearch -> Kibana
```

給剛接觸大數據與 Kafka Connect 的學生

---
layout: section
---

# 先從一個明確問題開始

---

# 電商平台需要追蹤什麼？

假設今晚 8 點有一個商品突然爆紅。

營運與工程團隊想立刻回答：

- 這個商品的流量是不是正在快速上升？
- 使用者是在瀏覽、點擊購買，還是一直重新整理？
- 購買或領券成功率是否下降？
- 失敗原因是售罄、限流，還是付款失敗？
- 哪些地區壓力最大？
- 是否有少數使用者出現高頻操作線索？

核心問題：

```text
熱門商品的各種情況，要如何被近即時追蹤與觀察？
```

---

# 觀測目標不是單筆訂單

交易系統通常關心：

- 訂單是否成立
- 庫存是否扣除
- 付款是否成功

熱門商品觀測則需要分析事件流：

- 每分鐘有多少瀏覽？
- 點擊量是否突然上升？
- 失敗事件是否集中在某個時間點？
- 售罄後使用者是否仍大量重試？

這類問題更接近：

```text
event search + time-series aggregation + dashboard
```

---
layout: section
---

# 先定義要追蹤的指標

---

# 為什麼要先定義 Metrics？

如果一開始沒有定義指標，後面會不知道：

- event 應該長什麼樣子
- Kafka topic 裡要放哪些欄位
- Elasticsearch 要怎麼查
- Kibana dashboard 要放哪些 panel
- demo 成功與否要怎麼判斷

因此順序應該是：

```text
業務問題 -> 指標 -> 事件模型 -> 資料管線 -> Dashboard
```

此順序能避免先建立 pipeline，再回頭推測 dashboard 應呈現哪些資料。

---

# 本 Demo 要追蹤的 Metrics

我們關心的是「熱門商品或限量折價券是否正在爆量，以及爆量後發生什麼事」。

| 指標 | 問題 |
| --- | --- |
| 事件總數 | 事件是否已被索引到 Elasticsearch？ |
| 事件類型趨勢 | 流量是在瀏覽、刷新、點擊、成功，還是失敗？ |
| 關鍵行為統計 | 成功、失敗與需求壓力事件各自累積多少？ |
| 失敗原因 | 是售罄、限流，還是付款失敗？ |
| 高頻操作線索 | 重複刷新或搶購失敗是否集中於少數使用者？ |
| 地區流量 | 哪些地區的壓力最高？ |

---

# 指標會反推事件欄位

要追蹤上述 metrics，event 至少需要：

```text
event_type
occurred_at
user_id
product_id / coupon_id
remaining_stock / remaining_coupons
failure_reason
metadata.region
```

因此，event model 不應任意設計。

Dashboard 想回答的問題，會直接決定 event 裡需要哪些欄位。

---
layout: section
---

# 為什麼不能只靠 Database？

---

# Database 適合交易，不適合承擔所有觀測查詢

Database 適合保存正式交易狀態：

- 訂單
- 付款
- 庫存
- 使用者資料

如果 dashboard 直接查詢交易 DB，熱門商品爆量時會有下列風險：

- 大量查詢可能影響交易系統。
- event-style 查詢通常不是交易 DB 的主要設計目標。
- 每分鐘聚合、失敗原因統計、地區流量分析會和交易 workload 混在一起。
- 歷史事件查詢與稽核資料可能讓主資料庫膨脹。

精確說法：

```text
不是 Database 不能存資料，
而是不應讓交易 DB 同時承擔所有近即時觀測壓力。
```

---

# 為什麼也不是 Application 直接寫 Elasticsearch？

Elasticsearch 適合搜尋、聚合與 dashboard。

但如果電商 application 同步寫 Elasticsearch：

```text
使用者請求
    |
    +-> 寫交易 DB
    |
    +-> 寫 Elasticsearch
```

application 需要自己處理：

- Elasticsearch indexing latency
- retry 與 backpressure
- 外部系統短暫失敗
- 壞資料
- 重送造成的重複寫入
- 寫入狀態與監控

問題不在於 Elasticsearch 不能 indexing。

限制在於：

```text
業務 application 不應直接承擔整條資料同步管線的責任。
```

---

# 需要一個中間層

application 層只應承擔必要責任：

```text
產生事件 -> 寫入 Kafka
```

後面的搜尋、觀測、dashboard 寫入，交給資料管線處理。

責任可拆分為：

- Application：處理使用者請求與業務邏輯。
- Kafka：接住事件流，提供緩衝與重放能力。
- Kafka Connect：把 Kafka events 寫到外部系統。
- Elasticsearch：提供事件搜尋與聚合。
- Kibana：提供可視化 dashboard。

---
layout: section
---

# Kafka 能做到什麼？

---

# Kafka 的角色：先承接事件流

Kafka 在這個 demo 中不是 dashboard，也不是資料庫。

它的角色是事件緩衝層：

- decoupling：application 不需要知道後面有哪些 consumer。
- buffering：Elasticsearch indexing 延遲升高時，事件仍可先保留在 Kafka。
- replay：需要重建 index 或重新消費時，可以從 topic 讀回來。
- partitioning：事件可以分散到多個 partition，提高消費平行度。
- durability：事件不只存在 application memory。

可整理為：

```text
Kafka 先保存事件，再由後續系統依各自速度消費。
```

---

# Demo 中的 Kafka Topic

我們的事件先進入：

```text
product.events
```

demo 支援兩組事件。

基本熱門商品事件：

- `PRODUCT_VIEWED`
- `BUY_CLICKED`
- `PURCHASE_SUCCEEDED`
- `PURCHASE_FAILED`

限量折價券 profile 事件：

- `COUPON_VIEWED`
- `PAGE_REFRESHED`
- `WAITING_ROOM_JOINED`
- `COUPON_CLAIM_SUCCEEDED`
- `COUPON_CLAIM_FAILED`

目前講座 demo 主要使用限量折價券 profile，因為它能清楚呈現刷新、排隊、售罄與失敗原因。

Kafka 只負責承接與保存事件流，不負責產生 dashboard，也不負責把資料寫入 Elasticsearch。

---
layout: section
---

# Kafka Connect 把兩邊串起來

---

# Kafka Connect 的角色

Kafka Connect 負責把 Kafka topic 接到外部系統。

在這個 demo 中：

```text
Kafka topic: product.events
        |
        v
Kafka Connect Elasticsearch Sink
        |
        v
Elasticsearch index: product-events
```

我們不是自己寫一支 Java consumer 來同步 Elasticsearch。

我們使用 Kafka Connect，因為它提供標準化的：

- connector lifecycle
- task model
- converter
- SMT
- DLQ
- status API
- internal topics

---

# 完整 Demo 架構

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
Kibana Dashboard
```

這條管線的目標不是只證明「有資料」。

重點是：

```text
資料流動過程可觀察、可維護、可重跑、可處理錯誤。
```

---
layout: section
---

# Demo：可重播的近即時觀測

---

# Demo 要讓學生看到什麼？

Demo 不只展示指令能否執行。

我們要讓學生在 dashboard 上看到：

- 事件總數快速累積。
- 事件類型隨時間變化。
- 成功與失敗比例改變。
- 售罄或限流造成失敗原因集中。
- 高頻操作線索浮現。
- 不同地區有不同流量壓力。

這些 panel 對應到一個真實問題：

```text
商品是不是正在爆量？
爆量後，系統與使用者遇到了什麼狀況？
```

---

# 實際 Demo 指令

啟動 stack：

```bash
just setup
```

產生可重跑的 profile-driven 流量。此流程使用固定時間窗產生資料，因此每次執行會得到一致結果：

```bash
just replay-demo
```

Dashboard：

```text
http://localhost:5601/app/dashboards#/view/hot-product-sales-dashboard
```

---

# Demo 模式說明

本 demo 採用可重播模式：

- 每次先清理 connector、topics、Connect internal topics 與 Elasticsearch index。
- 使用固定 `BASE_TIME=2026-05-01T12:00:00Z`。
- 重新產生 24,000 筆折價券搶購事件。
- Dashboard time range 會對齊固定事件時間窗。

因此，這個 demo 能穩定呈現 Kafka Connect pipeline 與 dashboard 結果。

它不是依照當下 wall-clock time 持續推進的動畫式流量模擬。

---

# Dashboard Panel Review

目前 dashboard 觀察項目：

| Panel | 目的 |
| --- | --- |
| 事件總數 | 確認事件已被索引到 Elasticsearch |
| 事件類型趨勢 | 看瀏覽、刷新、成功、失敗如何隨時間變化 |
| 關鍵行為統計 | 看成功、失敗、需求壓力事件的累積數量 |
| 失敗原因 | 看售罄、限流、付款失敗是否集中 |
| 高頻操作線索 | 觀察重複刷新或搶購失敗是否集中於少數使用者 |
| 地區流量 | 比較不同地區的壓力分布 |

`關鍵行為統計` 是 filter count，不是完整 conversion rate。pipeline health 需要搭配 Kafka Connect status API 與 E2E 檢查判斷。

---
layout: section
---

# Demo 之後，再拆第四章概念

---

# 第四章的核心問題

第四章不是只問：

```text
這個 connector 跑不跑得起來？
```

而是問：

- 這條 pipeline 是否容易維護？
- 發生故障時是否能觀察與定位？
- 資料量變大時是否能擴展？
- 資料格式改變時是否有演進策略？
- 外部系統失敗時是否會拖垮 application？

---

# Component 1：Connector 選型

先判斷資料方向：

```text
Source connector:
  外部系統 -> Kafka

Sink connector:
  Kafka -> 外部系統
```

我們的 demo 是 sink：

```text
Kafka -> Elasticsearch
```

原因：我們要把 Kafka events 變成可以搜尋、聚合與視覺化的資料。

---

# Component 2：Event Model

Dashboard 要回答什麼問題，event 就必須包含對應欄位。

範例：

```json
{
  "event_type": "COUPON_CLAIM_FAILED",
  "coupon_id": "coupon_mayday_001",
  "user_id": "user_01234",
  "occurred_at": "2026-05-01T12:00:00Z",
  "remaining_coupons": 0,
  "failure_reason": "COUPON_SOLD_OUT",
  "metadata": {
    "region": "ap-northeast-1"
  }
}
```

沒有明確 event model，dashboard 只能呈現模糊訊息。

---

# Component 3：Converter

Kafka 裡的資料本質上是 bytes。

Kafka Connect 需要 converter 把 bytes 轉成 ConnectRecord：

```text
Kafka bytes
    |
    | JsonConverter
    v
ConnectRecord
```

此 demo 使用 schemaless JSON。

教學取捨：

- 初學者容易看懂。
- Kibana 可以直接看到欄位。
- production 通常應納入 Schema Registry 與相容性規則。

---

# Component 4：SMT

SMT 是 Single Message Transform。

它適合做 record-local 的輕量轉換：

- 加欄位
- 改欄位名
- 刪欄位
- 展平欄位

我們 demo 使用：

```text
Flatten:
  metadata.region -> metadata_region

InsertField:
  pipeline=connect-search-demo
```

SMT 不適合放跨事件統計、join 或複雜商業邏輯。

---

# Component 5：Tasks 與 Partitions

Kafka Connect 的平行處理單位是 task。

但 task 不是越多越好。

```text
Kafka partitions
    |
    v
Kafka Connect sink tasks
```

Demo 設定：

```text
product.events topic: 3 partitions
Elasticsearch sink: tasks.max=2
```

有效平行度會受 partitions、connector 行為與外部系統限制。

---

# Component 6：DLQ

實務資料管線需要處理壞資料。

例如 malformed JSON：

```json
{"event_id":"bad_1",
```

問題是：

```text
整條 pipeline 要停止？
還是保存壞資料，讓主流程繼續？
```

Demo 設定：

```text
errors.tolerance=all
errors.deadletterqueue.topic.name=product.events.dlq
```

此 demo 驗證的是解析或轉換階段的壞資料會進入 DLQ。

DLQ 不代表資料問題消失；它代表問題資料被隔離，後續仍要監控與補償。外部系統故障、mapping conflict 或長時間 backpressure 仍需搭配 retry、監控與告警處理。

---

# Component 7：Internal Topics

Distributed mode 的 Kafka Connect 會使用 internal topics：

```text
connect-configs-hot-product-demo
connect-offsets-hot-product-demo
connect-status-hot-product-demo
```

用途：

- connector config
- offsets
- connector / task status

這表示 Kafka Connect 不是只靠 worker 本機狀態。

重要狀態會放回 Kafka。

---

# Component 8：Delivery Semantics

跨系統資料管線需要明確說明 delivery semantics。

此 demo 不宣稱：

```text
這條 pipeline 一定 exactly-once。
```

這個 demo 的精確說法：

```text
Elasticsearch sink 以 at-least-once 方式理解。
```

降低重送影響的方法：

```text
Kafka record key = event_id
Elasticsearch document id = key
write.method = upsert
```

這是 practical idempotency，不等於跨系統通用 exactly-once。

---
layout: section
---

# 回到整體架構

---

# 為什麼這樣設計？

```text
Application
  產生業務事件

Kafka
  接住事件流，提供 buffer 與 replay

Kafka Connect
  標準化地把 Kafka records 寫到外部系統

Elasticsearch
  提供 event search 與 aggregation

Kibana
  讓人看到趨勢與問題
```

這個設計不是為了增加架構複雜度。

其目的在於分離不同系統責任。

---

# 本章重點

Kafka Connect 不是能自動解決所有問題的黑盒。

它是一套標準化資料整合框架。

設計 pipeline 時要回答：

- 選哪個 connector？
- 資料方向是 source 還是 sink？
- event model 是否支援 dashboard 問題？
- SMT 只做輕量轉換嗎？
- tasks 與 partitions 是否合理？
- 壞資料去哪裡？
- 出事時如何觀察？
- 語意保證是否說得精確？

---

# 一句話總結

```text
熱門商品爆量時，
交易系統不應直接承擔搜尋與觀測壓力。

Kafka 先接住事件，
Kafka Connect 負責把事件穩定送到 Elasticsearch，
Kibana 讓團隊近即時觀察趨勢。
```

這就是本 demo 要呈現的資料管線設計。

---

# 附錄：可重跑與驗證

Demo seed scripts 預設會清理：

- connector
- Kafka data topics
- Kafka Connect internal topics
- Elasticsearch index

並使用固定時間：

```text
BASE_TIME=2026-05-01T12:00:00Z
```

因此每次 demo 都能得到一致的 dashboard 結果。

E2E 驗證：

```bash
just e2e
```
