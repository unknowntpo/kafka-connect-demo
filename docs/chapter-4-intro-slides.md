---
title: "Kafka Connect Chapter 4 - Designing Effective Data Pipelines"
audience: "Students new to big data"
format: "slide-friendly markdown"
demo: "Kafka -> Kafka Connect -> Elasticsearch -> Kibana"
---

# Kafka Connect Chapter 4

Designing Effective Data Pipelines

給完全不熟大數據的同學看的版本

---

# Opening Story

想像今天晚上 8 點有一批限量折價券開搶。

```text
很多人打開網頁
很多人一直按重新整理
一部分人成功領到券
券被領完後，更多人看到「已售完」
營運團隊想即時看到發生什麼事
```

問題是：這些事件要怎麼從系統送到 dashboard？

---

# The Demo Pipeline

我們的 demo 是一條資料輸送帶。

```text
Event Generator
    |
    v
Kafka topic: product.events
    |
    v
Kafka Connect
    |
    v
Elasticsearch
    |
    v
Kibana Dashboard
```

Kafka Connect 的角色：依照 connector 設定，把 Kafka 裡的資料持續送到外部系統。

---

# Chapter 4 Main Question

第四章不是只問：

```text
這個 connector 跑不跑得起來？
```

而是問：

```text
這條 pipeline 是否容易維護？
發生故障時是否能觀察與定位？
資料量變大時是否能擴展？
資料格式改變時是否有明確的演進策略？
```

---

# One Sentence Summary

Kafka Connect Chapter 4 is about designing pipelines that are:

- usable
- maintainable
- scalable
- observable
- failure-aware

中文講法：

```text
不是只把資料搬過去，而是要讓資料流動過程可觀察、可維護、可恢復。
```

---

# Mental Model

把 Kafka Connect 想成資料物流公司。

```text
Kafka = 倉庫
Connector = 貨車路線
Task = 真正開車送貨的人
Converter = 翻譯包裹格式的人
SMT = 在包裹上貼標籤的人
DLQ = 問題包裹暫存區
External system = 收貨地點
```

第四章可理解為：如何設計這套資料物流系統，使它在資料量、故障與格式變更下仍可運作。

---

# Design Question 1

## 要選哪一條資料路線？

也就是：Choosing a Connector

先問三件事：

- 資料方向是 source 還是 sink？
- connector 授權、維護、支援是否可信？
- connector 功能是否符合需求？

---

# Source vs Sink

```text
Source connector
外部系統 -> Kafka

Sink connector
Kafka -> 外部系統
```

我們的 demo 是 sink：

```text
Kafka -> Elasticsearch / Kibana
```

因為我們想把事件變成可以搜尋、可以視覺化的資料。

---

# Demo Mapping

為什麼選 Elasticsearch sink？

- Kibana 可以做 dashboard
- Elasticsearch 適合查詢事件
- 適合 log indexing、監控、稽核、事件搜尋
- 初學者可以直接確認資料已經寫入目標系統

這比把資料寫到某個看不見的系統更適合 demo。

---

# Design Question 2

## 資料長什麼樣子？

也就是：Defining Data Models

資料模型會直接影響查詢、擴展與除錯。

如果 dashboard 要回答「折價券是不是被搶爆了」，事件就應該有：

- `event_type`
- `occurred_at`
- `user_id`
- `coupon_id`
- `remaining_coupons`
- `failure_reason`

---

# Bad Event Example

這種資料不利於建立 dashboard：

```json
{
  "message": "user did something"
}
```

問題：

- 不知道什麼時候發生
- 不知道哪個使用者
- 不知道事件類型
- 不知道成功或失敗
- 不知道折價券還剩幾張

---

# Better Event Example

```json
{
  "event_type": "COUPON_CLAIM_FAILED",
  "coupon_id": "coupon_mayday_001",
  "user_id": "user_01234",
  "occurred_at": "2026-04-30T12:00:00Z",
  "remaining_coupons": 0,
  "failure_reason": "COUPON_SOLD_OUT"
}
```

這樣 dashboard 才能回答具體問題：

- 什麼事件最多？
- 什麼時候爆量？
- 是誰在操作？
- 失敗原因是什麼？

---

# Design Question 3

## 轉換要放哪裡？

Chapter 4 提到 ETL vs ELT。

```text
ETL: Extract -> Transform -> Load
ELT: Extract -> Load -> Transform
```

Kafka Connect 也可以做 transformation，但只適合輕量操作。

---

# SMT Is Not Business Logic

Kafka Connect SMT 適合：

- 加欄位
- 改欄位名
- 刪欄位
- 簡單 route

不適合承擔：

- 跨事件統計
- join
- 複雜規則引擎
- 機器學習推論

我們 demo 的 SMT 做兩件 record-local 的事：

```text
Flatten:
  metadata.region -> metadata_region
  metadata.campaign -> metadata_campaign

InsertField:
  pipeline=connect-search-demo
```

商業意義：Kibana 可以直接用 `metadata_region` 比較不同地區的搶券壓力與失敗原因。

---

# Why This Matters

如果把太多商業邏輯放進 Kafka Connect：

- pipeline 難以測試
- connector config 會變成難以測試的隱藏邏輯
- 故障時難以判斷責任邊界
- 未來更換 connector 或調整 pipeline 會增加維護成本

實務原則：

```text
Kafka Connect 搬資料。
Kafka Streams / Flink / Spark 處理複雜邏輯。
```

---

# Design Question 4

## 要設定多少個 task？

也就是：Tasks and Partitions

Kafka Connect 的平行處理單位是 task。

但 task 不是越多越好。

---

# Tasks vs Partitions

Sink connector 讀 Kafka topic。

```text
Kafka partitions
    |
    v
Kafka Connect sink tasks
```

同一個 partition 同一時間只會被一個 task 讀。

所以：

```text
partition 數量會限制 sink task 的有效並行度
```

---

# Demo Mapping

我們的 demo：

```text
product.events topic: 3 partitions
Elasticsearch sink: tasks.max=2
```

這可以用來說明：

- `tasks.max=2` 代表最多 2 個 sink task
- 不是設成 100 就會快 100 倍
- 實際平行度受 partitions、connector、外部系統限制

---

# Design Question 5

## 資料格式誰負責？

Chapter 4 把責任拆成三層：

```text
Connector
    定義 Kafka Connect 如何和外部系統互動

Converter
    Kafka bytes <-> ConnectRecord

Transformation
    ConnectRecord -> ConnectRecord
```

---

# Sink Pipeline Flow

我們的 demo 是 sink pipeline。

```text
Kafka bytes
    |
    | JsonConverter
    v
ConnectRecord
    |
    | SMT: Flatten + InsertField
    v
ConnectRecord
    |
    | Elasticsearch sink task
    v
Elasticsearch document
```

這對應 Chapter 4 的 data format 責任分工：converter 處理 bytes 與 `ConnectRecord` 的轉換，SMT 修改 `ConnectRecord`，sink connector 將資料寫入目標系統。

---

# Design Question 6

## Schema 要不要管？

Schema 的作用：

```text
讓資料不只是 bytes，而是有欄位、有型別、有演進規則。
```

真實系統常用：

- Avro
- JSON Schema
- Protobuf
- Schema Registry

---

# Our Demo Choice

我們 demo 目前用 schemaless JSON。

原因：

- 初學者比較好理解
- Kibana 可以直接看到欄位
- 重點放在 Kafka Connect pipeline design

限制在於：

```text
正式 production pipeline 通常應納入 schema registry 與相容性規則。
```

---

# Design Question 7

## Kafka Connect 怎麼記住狀態？

Distributed mode 依賴 internal topics：

```text
connect-configs
connect-offsets
connect-status
```

它們分別保存：

- connector config
- offsets
- connector/task status

---

# Demo Mapping

我們的 E2E 會檢查：

```text
connect-configs-hot-product-demo
connect-offsets-hot-product-demo
connect-status-hot-product-demo
```

這代表 Kafka Connect distributed mode 不只依賴單一 worker 的本機狀態。

它把重要狀態放進 Kafka。

---

# Design Question 8

## 壞資料怎麼辦？

現實世界一定會有壞資料。

例如：

```json
{"event_id":"bad_1",
```

這是一筆壞掉的 JSON。

設計問題：

```text
整條 pipeline 是否要停止？
或是先保存壞資料，讓主流程繼續處理？
```

---

# Dead Letter Queue

DLQ 是 sink pipeline 的問題資料暫存區。

```text
可處理資料 -> Elasticsearch
無法處理資料 -> product.events.dlq
```

我們 demo 設定：

```text
errors.tolerance=all
errors.deadletterqueue.topic.name=product.events.dlq
```

---

# Why DLQ Is Useful

沒有 DLQ：

```text
一筆壞資料可能讓 task 進入 FAILED 狀態
```

有 DLQ：

```text
主流程可以繼續處理其他 records
壞資料被保存到 DLQ topic
後續可由人工或另一個 consumer / connector 補處理
```

代價：

```text
DLQ 不代表資料問題消失。
DLQ topic 仍需要監控與補償流程。
```

---

# Design Question 9

## 送資料的保證是什麼？

Chapter 4 提到三種語意：

- at-most-once
- at-least-once
- exactly-once

報告時需要避免過度承諾：

```text
不要把所有 pipeline 都描述成 exactly-once。
```

---

# Our Demo Semantics

這個 demo 的精確說法：

```text
Elasticsearch sink 以 at-least-once 方式理解。
```

可能發生：

```text
同一筆資料被重送
```

我們降低影響的方法：

```text
Kafka record key = event_id
Elasticsearch document id = key
write.method = upsert
```

這是 practical idempotency：透過穩定 document id 降低重送造成的重複寫入影響，但不等同於跨系統通用的 exactly-once。

---

# Demo Moment

現在看 Kibana dashboard。

觀察：

- Total events
- Event volume by type
- Business outcomes
- Failure reasons
- Active users

用一句話解釋：

```text
我們不是只看到資料有來，而是看到事件趨勢和失敗原因。
```

---

# AI Load Generator Story

為了讓 dashboard 更像真實世界，我們做了 profile-driven load generator。

AI 不負責逐筆產生 event。

AI 負責生成「流量劇本」：

```text
teaser -> waiting-room -> drop-open -> sold-out-pressure
```

Java generator 負責依照 profile 穩定、可重跑地產生事件。

---

# Flash-Sale Coupon Profile

```text
24,000 events
80 minutes
1,200 coupons
many users refresh pages
claims succeed until inventory is zero
sold-out failures spike later
```

這讓 dashboard 不是 200 筆假資料，而是有趨勢、有壓力、有失敗原因的資料。

---

# Feedback Loop

我們不只依賴目測判斷資料是否合理。

`score-load-profile.sh` 會查 Elasticsearch：

- 總資料量是否足夠
- 後段流量是否明顯高於前段
- refresh / waiting room 是否存在
- coupon inventory 是否已經歸零
- sold-out failure 是否主導後段
- user 分布是否合理
- SMT 產生的 `metadata_region` 與 `pipeline` 欄位是否存在

---

# What Students Should Remember

Kafka Connect 不是無條件保證正確性的黑盒。

它是一套標準化資料搬運框架。

要設計好 pipeline，需要回答：

- 選哪個 connector？
- 資料方向是 source 還是 sink？
- 資料模型長什麼樣？
- transformation 放哪裡？
- task 和 partition 怎麼搭配？
- 壞資料怎麼處理？
- 出事怎麼看？
- 語意保證是什麼？

---

# The Big Picture

```text
Good pipeline design
    = correct connector
    + clear data model
    + appropriate parallelism
    + explicit failure handling
    + honest processing semantics
    + observable results
```

這就是 Kafka Connect 第四章的核心。

---

# Closing

如果只記一句話：

```text
Kafka Connect 不只是把資料搬過去；
它要求你設計一條可觀察、可維護、可恢復的資料管線。
```

Demo:

```bash
./scripts/seed-ai-load-profile.sh
```

這個腳本會先清除 connector、topics、Connect internal topics 與 Elasticsearch index，並使用固定 base time 重新產生資料，因此每次 demo 的統計結果一致。

Dashboard:

```text
http://localhost:5601/app/dashboards#/view/hot-product-sales-dashboard
```
