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

想像今天晚上 8 點有一張限量折價券開搶。

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

Kafka Connect 的角色：把 Kafka 裡的資料可靠地搬到外部系統。

---

# Chapter 4 Main Question

第四章不是只問：

```text
這個 connector 跑不跑得起來？
```

而是問：

```text
這條 pipeline 設計得好不好？
壞掉時看不看得出來？
資料量變大時撐不撐得住？
資料格式改變時會不會爆炸？
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
不是只把資料搬過去，而是要搬得穩、搬得清楚、出事能查。
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

第四章就是在教我們怎麼設計這間物流公司。

---

# Design Question 1

## 你要選哪一台貨車？

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
- 很容易讓初學者看到「資料真的過去了」

這比把資料寫到某個看不見的系統更適合 demo。

---

# Design Question 2

## 資料長什麼樣子？

也就是：Defining Data Models

資料模型不是小事。

如果 dashboard 要回答「折價券是不是被搶爆了」，事件就應該有：

- `event_type`
- `occurred_at`
- `user_id`
- `coupon_id`
- `remaining_coupons`
- `failure_reason`

---

# Bad Event Example

這種資料很難做 dashboard：

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

這樣 dashboard 才能回答：

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

不適合：

- 跨事件統計
- join
- 複雜規則引擎
- 機器學習推論

我們 demo 的 SMT 只做一件事：

```text
加上 pipeline=connect-search-demo
```

---

# Why This Matters

如果把太多商業邏輯塞進 Kafka Connect：

- pipeline 很難測
- connector config 變成隱藏程式碼
- 故障時很難知道誰錯
- 未來換 connector 會很痛

實務原則：

```text
Kafka Connect 搬資料。
Kafka Streams / Flink / Spark 處理複雜邏輯。
```

---

# Design Question 4

## 要開幾個工人？

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
    定義外部系統資料怎麼變成 ConnectRecord

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
    | SMT: InsertField
    v
ConnectRecord
    |
    | Elasticsearch sink task
    v
Elasticsearch document
```

這就是 Chapter 4 的 data format 概念。

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

但要誠實說：

```text
正式 production pipeline 通常應該認真考慮 schema registry。
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

這代表 Kafka Connect 不是只靠本機記憶體。

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

問題：

```text
整條 pipeline 要停掉嗎？
還是先把壞資料放到旁邊？
```

---

# Dead Letter Queue

DLQ 是問題包裹暫存區。

```text
正常資料 -> Elasticsearch
壞資料   -> product.events.dlq
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
一筆壞資料可能讓 task fail
```

有 DLQ：

```text
主流程繼續跑
壞資料被保存
之後可以人工或程式補處理
```

代價：

```text
你不能假裝資料沒有問題。
DLQ 也需要監控。
```

---

# Design Question 9

## 送資料的保證是什麼？

Chapter 4 提到三種語意：

- at-most-once
- at-least-once
- exactly-once

最重要的是：

```text
不要亂講 exactly-once。
```

---

# Our Demo Semantics

我們 demo 的說法：

```text
Elasticsearch sink is at-least-once.
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

這叫 practical idempotency，不是真正的 universal exactly-once。

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

AI 不負責一筆一筆生 event。

AI 負責生成「流量劇本」：

```text
teaser -> waiting-room -> drop-open -> sold-out-pressure
```

Java generator 負責照劇本穩定執行。

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

我們不是靠肉眼說「看起來很真」。

`score-load-profile.sh` 會查 Elasticsearch：

- 總資料量是否足夠
- 後段流量是否明顯高於前段
- refresh / waiting room 是否存在
- coupon 是否真的售罄
- sold-out failure 是否主導後段
- user 分布是否合理
- SMT 欄位是否存在

---

# What Students Should Remember

Kafka Connect 不是魔法。

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

Dashboard:

```text
http://localhost:5601/app/dashboards#/view/hot-product-sales-dashboard
```
