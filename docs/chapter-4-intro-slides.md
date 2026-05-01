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

# 電商平台想知道什麼？

假設今晚 8 點有一個商品突然爆紅。

營運與工程團隊想立刻回答：

- 這個商品的流量是不是正在快速上升？
- 使用者是在瀏覽、點擊購買，還是一直重新整理？
- 購買或領券成功率是否下降？
- 失敗原因是售罄、限流，還是付款失敗？
- 哪些地區壓力最大？
- 是否有少數使用者造成異常高頻操作？

核心問題：

```text
熱門商品的各種情況，要怎麼被即時追蹤與觀察？
```

---

# 我們要觀察的不是一筆訂單

交易系統通常關心：

- 訂單是否成立
- 庫存是否扣除
- 付款是否成功

但熱門商品觀測還需要看事件流：

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

不是先做 pipeline，再回頭猜要看什麼。

---

# 本 Demo 要追蹤的 Metrics

我們關心的是「熱門商品或限量折價券是否正在爆量，以及爆量後發生什麼事」。

| 指標 | 問題 |
| --- | --- |
| 事件總數 | Kafka Connect 是否持續把事件寫入 Elasticsearch？ |
| 事件類型趨勢 | 流量是在瀏覽、刷新、點擊、成功，還是失敗？ |
| 成功 / 失敗 / 需求壓力 | 使用者需求是否高於系統或庫存可承受範圍？ |
| 失敗原因 | 是售罄、限流，還是付款失敗？ |
| 高頻操作使用者 | 是否有重複刷新、搶購失敗或疑似 bot 行為？ |
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

這就是為什麼 event model 不是隨便設計的。

Dashboard 想回答的問題，會直接決定 event 裡需要哪些欄位。

---
layout: section
---

# 為什麼不能只靠 Database？

---

# Database 適合交易，不適合承擔所有觀測查詢

Database 很適合保存正式狀態：

- 訂單
- 付款
- 庫存
- 使用者資料

但如果 dashboard 直接查交易 DB，熱門商品爆量時會有風險：

- 大量查詢可能影響交易系統。
- event-style 查詢通常不是交易 DB 的主要設計目標。
- 每分鐘聚合、失敗原因統計、地區流量分析會和交易 workload 混在一起。
- 歷史事件查詢與稽核資料可能讓主資料庫膨脹。

精確說法：

```text
不是 Database 不能存資料，
而是不應讓交易 DB 同時承擔所有即時觀測壓力。
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

問題是：

```text
業務 application 不應直接承擔整條資料同步管線的責任。
```

---

# 需要一個中間層

我們希望 application 做最少的事情：

```text
產生事件 -> 寫入 Kafka
```

後面的搜尋、觀測、dashboard 寫入，交給資料管線處理。

這樣可以把責任切開：

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

# Kafka 的角色：先把事件穩定接住

Kafka 在這個 demo 中不是 dashboard，也不是資料庫。

它的角色是事件緩衝層：

- decoupling：application 不需要知道後面有哪些 consumer。
- buffering：Elasticsearch 慢一點時，事件仍先留在 Kafka。
- replay：需要重建 index 或重新消費時，可以從 topic 讀回來。
- partitioning：事件可以分散到多個 partition，提高消費平行度。
- durability：事件不只存在 application memory。

一句話：

```text
Kafka 讓事件先被可靠接住，再交給後面的系統慢慢處理。
```

---

# Demo 中的 Kafka Topic

我們的事件先進入：

```text
product.events
```

裡面包含熱門商品與限量折價券事件：

- `PRODUCT_VIEWED`
- `BUY_CLICKED`
- `PURCHASE_SUCCEEDED`
- `PURCHASE_FAILED`
- `PAGE_REFRESHED`
- `COUPON_CLAIM_SUCCEEDED`
- `COUPON_CLAIM_FAILED`

Kafka 只負責承接與保存事件流。

它不負責產生 dashboard，也不負責把資料寫進 Elasticsearch。

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

這條管線的重點不是「有資料」。

重點是：

```text
資料流動過程可觀察、可維護、可重跑、可處理錯誤。
```

---
layout: section
---

# Demo：即時流量進 Dashboard

---

# Demo 要讓學生看到什麼？

Demo 不只是跑指令。

我們要讓學生在 dashboard 上看到：

- 事件總數快速累積。
- 事件類型隨時間變化。
- 成功與失敗比例改變。
- 售罄或限流造成失敗原因集中。
- 高頻操作使用者浮現。
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
./scripts/start.sh
./scripts/wait-for-connect.sh
./scripts/create-topics.sh
./scripts/create-search-resources.sh
./scripts/register-connectors.sh
./scripts/create-kibana-dashboard.sh
```

產生可重跑的 AI profile-driven 流量：

```bash
./scripts/seed-ai-load-profile.sh
```

Dashboard：

```text
http://localhost:5601/app/dashboards#/view/hot-product-sales-dashboard
```

---

# Dashboard Panel Review

目前 dashboard 觀察項目：

| Panel | 目的 |
| --- | --- |
| 事件總數 | 確認 Kafka Connect 已將事件寫入 Elasticsearch |
| 事件類型趨勢 | 看瀏覽、刷新、成功、失敗如何隨時間變化 |
| 業務結果 | 看成功、失敗、需求壓力的整體比例 |
| 失敗原因 | 看售罄、限流、付款失敗是否集中 |
| 高頻操作使用者 | 找出重複刷新、搶購失敗或疑似 bot 行為 |
| 地區流量 | 比較不同地區的壓力分布 |

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

Dashboard 想回答什麼，event 就要包含對應欄位。

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

沒有明確 event model，dashboard 只能看到模糊訊息。

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

這個 demo 使用 schemaless JSON。

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

現實世界一定會有壞資料。

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

DLQ 不代表資料問題消失；它代表問題資料被隔離，後續仍要監控與補償。

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

報告時要避免過度承諾。

不要說：

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

這不是為了把架構變複雜。

而是為了把不同責任拆開。

---

# 學生應該記住什麼？

Kafka Connect 不是神奇黑盒。

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
Kibana 讓我們即時觀察趨勢。
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
./scripts/e2e.sh
```
