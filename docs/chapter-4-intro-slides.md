---
theme: default
title: Kafka Connect 第四章 - 用限量折價券搶領觀測設計資料管線
info: |
  這份 Slidev 投影片用熱門商品帶動限量折價券搶領的觀測情境，介紹 Kafka、Kafka Connect、Elasticsearch 與 Kibana 如何組成一條可觀察的資料管線。
class: text-left
drawings:
  persist: false
transition: slide-left
mdc: true
---

# Kafka Connect 第四章

用限量折價券搶領觀測理解資料管線設計

```text
限量折價券搶領事件如何進入 dashboard？
```

給剛接觸大數據與 Kafka Connect 的學生

---
layout: section
---

# 問題場景：限量折價券八點開搶

---

# 團隊真正想知道什麼？

假設平台今晚八點推出限量折價券，並在八點開放搶購。

營運與工程團隊需要快速做出判斷：

- 折價券活動流量是不是正在快速上升？
- 領券失敗原因是售罄還是限流？
- 哪些地區壓力最大？

這些判斷，就是 dashboard 要提供的 insight。

因此設計順序應該是：

```text
Insight -> 指標 -> 事件 -> 資料管線
```

---

# 把 Insight 轉成指標

把前一頁的 insight 轉成可觀察的指標：

- 流量是否正在快速上升？
- 使用者是在瀏覽活動頁、重新整理、排隊，還是領券？
- 領券成功率是否下降？
- 失敗原因是售罄還是限流？
- 哪些地區壓力最大？
- 是否有少數使用者出現高頻操作線索？

核心問題：

```text
熱門商品帶動折價券搶領時，哪些指標能幫助團隊近即時判斷狀況？
```

---

# 指標可以分為兩類

第一類是正式活動結果。

活動系統通常關心：

- 使用者是否符合資格
- 折價券是否扣除
- 領券紀錄是否成立

第二類是活動觀測狀態。

這類問題需要分析事件流：

- 每分鐘有多少瀏覽？
- 重新整理是否突然上升？
- 領券失敗是否集中在某個時間點？
- 售罄後使用者是否仍大量重試？

這個 demo 接下來聚焦限量折價券搶領狀態。

---
layout: section
---

# 選定這個 demo 的觀測指標

---

# 為什麼要先選定觀測指標？

先定義兩個接下來會反覆出現的詞：

```text
event = 一筆使用者或系統行為紀錄
資料管線 = 把事件從產生端送到查詢或視覺化系統的一連串處理步驟
```

如果一開始沒有先決定 insight 與指標，後面會不知道：

- event 應該長什麼樣子
- 資料管線需要保存哪些資訊
- 查詢系統要怎麼查
- dashboard 要放哪些圖
- 資料管線是否真的支援觀測與排查

這個順序能避免先建立 pipeline，再回頭推測 dashboard 應呈現哪些資料。

---

# 這個 demo 要追蹤的觀測指標

把想得到的 insight 轉成可計算的指標：

| 指標 | 問題 |
| --- | --- |
| 事件總數 | 事件是否已進入查詢系統？ |
| 事件類型 | 流量是在瀏覽、重新整理、點擊、成功，還是失敗？ |
| 關鍵行為統計 | 成功、失敗與頁面重新整理事件各自累積多少？ |
| 失敗原因 | 是售罄還是限流？ |
| 高頻操作線索 | 反覆重新整理或領券失敗是否集中於少數使用者？ |
| 地區流量 | 哪些地區的事件量最高？ |

---

# 指標由事件形成

dashboard 上的指標，通常由大量事件計算而來：

```text
單筆事件
  -> 依時間、類型、地區、折價券分組
  -> 計算 count / rate / top N
  -> 形成 dashboard 指標
```

例如：

- 每分鐘瀏覽量 = 每分鐘 `COUPON_VIEWED` 事件數
- 失敗原因分布 = 依 `failure_reason` 分組後計數
- 地區流量 = 依 `metadata.region` 分組後計數

因此，先決定指標，才能回推事件需要包含哪些欄位。

---

# 指標會反推事件欄位

在這個 demo 中，一筆 event 可以是：

- 使用者瀏覽折價券活動頁
- 使用者重新整理頁面
- 使用者進入等候室
- 使用者領券成功
- 使用者領券失敗

---

# Event 需要哪些欄位？

要追蹤上述 metrics，event 至少需要：

```text
event_type
occurred_at
user_id
coupon_id
remaining_coupons
failure_reason
metadata.region
```

---

# 欄位如何對應指標？

| 欄位 | 範例值 | 支援的指標 |
| --- | --- | --- |
| `event_type` | `PAGE_REFRESHED` | 事件類型 |
| `occurred_at` | `2026-05-01T20:00:15Z` | 每分鐘流量 |
| `user_id` | `user_01883` | 高頻操作線索 |
| `coupon_id` | `coupon_may_sale` | 指定折價券觀測 |
| `remaining_coupons` | `0` | 售罄壓力 |
| `failure_reason` | `RATE_LIMITED` | 失敗原因 |
| `metadata.region` | `TW-NORTH` | 地區流量 |

Dashboard 想回答的問題，會直接決定 event 裡需要哪些欄位。

---
layout: section
---

# 下一個問題：資料要放在哪裡？

---

# 先看整個 Service 要做什麼

限量折價券搶領觀測看起來是一個 dashboard。

實際上，背後至少包含四種工作：

| 工作 | 例子 |
| --- | --- |
| 處理活動狀態 | 建立領券紀錄、扣除折價券數量、套用限流 |
| 建立可查詢資料 | 把事件整理成適合搜尋與聚合的格式 |
| 查詢事件 | 查某段時間的瀏覽、重新整理、成功、失敗 |
| 聚合與統計 | 計算每分鐘流量、失敗原因、地區分布 |

---

# 活動流程也會產生 Event

處理領券流程時通常也會產生 event：

| 輸出 | 用途 |
| --- | --- |
| 活動結果 | 保存領券紀錄、剩餘券數、限流結果 |
| 事件紀錄 | 送進資料管線，用於觀測 |

接下來的架構選擇要回答：

```text
這些工作要由同一個系統承擔，還是拆給不同系統負責？
```

---

# 從 Service 責任走到資料架構

到這裡，我們只完成第一步：

```text
知道要觀察什麼
知道 event 應該長什麼樣子
```

但還沒有回答第二步：

```text
這些 event 要寫到哪裡？
誰負責讓 dashboard 查得到？
```

接下來進入架構選擇。

先把候選方案列出來，再檢查它們如何分配 service 責任：

- 直接查正式狀態 Database
- Application 直接寫搜尋系統
- 透過 Kafka 與 Kafka Connect 建立資料管線

---

# 第一個候選方案：直接查正式狀態 DB

---

# Database 適合承擔正式狀態

在這四種工作裡，正式狀態 Database 最適合承擔「處理活動狀態」與「保存正式狀態」：

- 領券紀錄
- 剩餘券數
- 使用者資格
- 限流結果

這些資料代表正式業務結果，需要正確性與一致性。

---

# 觀測查詢壓在正式狀態 DB 的風險

如果同一個正式狀態 DB 還要承擔「查詢事件」與「聚合統計」，折價券搶領爆量時會有下列風險：

- 大量查詢可能影響領券流程。
- event-style 查詢和正式狀態 DB 的主要設計目標不同。
- 每分鐘聚合、失敗原因統計、地區流量分析會和正式狀態 workload 混在一起。
- 長期事件明細、查詢索引與 dashboard 聚合會增加正式狀態 DB 的負擔。

設計判斷：

```text
正式狀態 DB 保存活動結果與必要稽核紀錄。
事件查詢與 dashboard 聚合需要獨立評估負載、資料量與查詢型態。
```

---

# 不是所有歷史資料都要搬走

領券紀錄、扣券結果、必要審計欄位通常仍會保存在正式狀態 DB。

需要另外評估的是：

- 大量事件明細
- 搜尋索引
- 時間序列聚合
- dashboard 查詢模型

這些資料常會複製到 read-optimized store 或觀測系統。

---

# 觀測查詢需要另一種系統

事件查詢與聚合統計需要的能力更接近：

- 事件搜尋
- 時間序列統計
- dashboard 查詢

因此下一個候選方案是：

```text
把 event 寫進搜尋與聚合系統
```

---

# 第二個候選方案：Application 直接寫 Elasticsearch

---

# Elasticsearch 在這裡的角色

Elasticsearch 適合承擔三種工作：

- 建立可查詢資料
- 查詢事件
- 聚合與統計

在這個 demo 中，Elasticsearch index 可以想成：

```text
為事件查詢與 dashboard 準備好的資料表
```

新的問題是：

```text
event 要怎麼從 application 穩定進入 Elasticsearch？
```

---

# Application 同步寫 Elasticsearch 的代價

如果電商 application 同步寫 Elasticsearch：

```text
使用者請求
    |
    +-> 寫正式狀態 DB
    |
    +-> 寫 Elasticsearch
```

application 需要自己處理：

- 寫入搜尋系統可能變慢。
- 失敗後要重試。
- 下游變慢時，使用者請求可能被迫等待。
- 外部系統短暫失敗
- 重送造成的重複寫入

使用者請求會受到外部系統速度、重試與錯誤處理影響。

---

# 需要一個中間層

application 層保留兩個必要責任：

```text
產生 event
寫入一個可緩衝、可重放的地方
```

後面的搜尋、觀測、dashboard 寫入，交給資料管線處理。

這會導向第三個候選方案：

```text
Application -> Kafka -> Kafka Connect -> Elasticsearch
```

第一步先引入 Kafka：

```text
Application -> Kafka
```

此時先不討論 dashboard，也先不討論 Kafka Connect。

---
layout: section
---

# Kafka 能做到什麼？

---

# Kafka 的第一個角色：承接事件流

Kafka 在這個 demo 中扮演事件流入口。

```text
Application
    |
    | event
    v
Kafka
```

這樣 application 不需要同步等待 dashboard 系統完成寫入。

它只需要先把事件送出。

---

# Topic 是什麼？

Kafka 會把 event 放進 topic。

```text
topic = 一條有名字的事件流
```

這個 demo 的 topic：

```text
product.events
```

可以把它想成：

```text
所有折價券搶領事件都先排進 product.events
```

---

# Kafka 的第二個角色：緩衝與重放

Kafka 提供兩個重要能力：

- buffering：後面的系統變慢時，事件仍可先保留在 Kafka。
- replay：後面的系統需要重建資料時，可以重新讀 topic。

因此，application 與觀測系統不需要同步綁在一起。

```text
Application -> Kafka -> 後續系統
```

Kafka 先保存事件，再由後續系統依各自速度消費。

---

# 這條資料管線的 Events

這條資料管線主要使用限量折價券事件：

- `COUPON_VIEWED`
- `PAGE_REFRESHED`
- `WAITING_ROOM_JOINED`
- `COUPON_CLAIM_SUCCEEDED`
- `COUPON_CLAIM_FAILED`

這組事件能清楚呈現重新整理、排隊、售罄與失敗原因。

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
Kafka Connect
        |
        v
Elasticsearch index: product-events
```

此處選擇 Kafka Connect 來同步 Elasticsearch。

先用一句話理解：

```text
Kafka Connect = Kafka 與外部系統之間的標準資料搬運層
```

這個 demo 的外部系統是 Elasticsearch。

---

# 完整 Demo 架構

```text
Java 事件產生器
        |
        | JSON events
        v
Kafka topic: product.events
        |
        | Kafka Connect
        v
Elasticsearch index: product-events
        |
        v
Kibana Dashboard
```

這條管線的目標超過「有資料」。

重點是：

```text
資料流動過程可觀察、可維護、可重跑、可處理錯誤。
```

---
layout: section
---

# 可重播的近即時觀測流程

---

# Dashboard 要呈現什麼？

真實資料管線不能只確認指令有執行。

它需要讓 dashboard 呈現可判讀的觀測結果：

- 事件總數快速累積。
- 事件類型隨時間變化。
- 成功與失敗比例改變。
- 售罄或限流造成失敗原因集中。
- 高頻操作線索浮現。
- 不同地區有不同流量壓力。

這些 panel 對應到一個真實問題：

```text
折價券搶領是不是正在爆量？
爆量後，系統與使用者遇到了什麼狀況？
```

---

# 實際 Demo 指令

啟動 stack：

```bash
just setup
```

產生一組固定劇本的流量。此流程使用固定時間窗產生資料，因此每次執行會得到一致結果：

```bash
just replay-demo
```

Dashboard：

```text
http://localhost:5601/app/dashboards
```

---

# Demo 模式說明

這個 demo 採用可重播模式：

- 每次先清理上一輪 demo 狀態。
- 使用固定 `BASE_TIME=2026-05-01T12:00:00Z`。
- 重新產生 24,000 筆折價券搶領事件。
- Dashboard time range 會對齊固定事件時間窗。

因此，這條流程能穩定呈現 Kafka Connect pipeline 與 dashboard 結果。

它採用固定時間窗，重點是可重跑與結果一致。

---

# Dashboard Panel Review

目前 dashboard 觀察項目：

| Panel | 目的 |
| --- | --- |
| 事件總數 | 確認事件已進入查詢系統 |
| 事件類型 | 看瀏覽、重新整理、成功、失敗如何隨時間變化 |
| 關鍵行為統計 | 看成功、失敗、頁面重新整理事件的累積數量 |
| 失敗原因 | 看售罄或限流是否集中 |
| 高頻操作線索 | 觀察反覆重新整理或領券失敗是否集中於少數使用者 |
| 地區流量 | 比較不同地區的壓力分布 |

---

# Panel 不能代表全部健康狀態

`關鍵行為統計` 是依條件分組後的事件數，目前比較成功、失敗與 `PAGE_REFRESHED`。

完整領券成功率需要額外定義嘗試次數與成功次數。

資料管線是否健康，還需要搭配：

- connector / task 狀態
- Elasticsearch 寫入結果
- 端到端檢查

---
layout: section
---

# Demo 之後，再拆第四章概念

---

# 接下來才進入第四章術語

前面先建立直覺：

```text
event -> topic -> Kafka -> Kafka Connect -> Elasticsearch
```

接下來開始拆 Kafka Connect 的設計問題。

每個術語都對應到 demo 中的一個實際零件。

---

# 第四章的核心主軸

先抓住一句話：

```text
資料管線要能跑，
也要能被維護、觀察與安全重送。
```

接下來只看五個問題：

- 方向：資料往哪裡走？
- 格式：怎麼解析與轉換？
- 錯誤：壞資料去哪裡？
- 狀態：出事時看哪裡？
- 重送：重複寫入怎麼辦？

接下來每個設計問題只處理其中一個面向。

---

# 設計問題 1：Connector

第一個術語是 connector。

```text
connector = Kafka Connect 用來連接外部系統的元件
```

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

# 設計問題 2：Event Model

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

# 設計問題 3：Converter

Kafka 裡的資料本質上是 bytes。

Kafka Connect 需要 converter。

Kafka 只保存位元資料。Connect 要先把資料解析成有欄位的 record，後面的 SMT 與 sink 才知道怎麼處理。

```text
ConnectRecord = Kafka Connect 內部用來表示一筆資料的標準格式
converter = Kafka bytes 與 ConnectRecord 之間的轉換器
```

---

# Converter 在 Sink Pipeline 的位置

Sink pipeline 中的流程：

```text
Kafka bytes
    |
    | JsonConverter
    v
ConnectRecord
```

此 demo 使用 schemaless JSON。

schemaless JSON 指的是不附帶 schema 定義的 JSON。

此處的設計取捨：

- JSON event 容易直接觀察。
- Kibana 可以直接看到欄位。
- production 通常還需要明確的 schema 與相容性規則。

---

# 設計問題 4：SMT 是什麼？

SMT 是 Single Message Transform。

```text
SMT = 對單筆 record 做輕量轉換
```

SMT 只看當下這一筆資料。

它適合做輕量轉換：

- 加欄位
- 改欄位名
- 刪欄位
- 展平欄位

SMT 不會拿多筆事件一起計算，也不會查其他資料表。

---

# 為什麼這個 demo 需要 SMT？

Application 送出的 event 保留業務語意。

Elasticsearch 與 Kibana 需要適合查詢與分組的欄位。

SMT 負責在 Connect pipeline 裡做這層輕量調整。

```text
application event -> Connect SMT -> search-friendly document
```

---

# 這個 demo 的 SMT

我們使用兩個 SMT：

| SMT | 轉換 | 目的 |
| --- | --- | --- |
| `Flatten` | `metadata.region` -> `metadata_region` | 讓 dashboard 直接依地區分組 |
| `InsertField` | `pipeline=connect-search-demo` | 標記資料由這條 Connect pipeline 寫入 |

```text
metadata.region       metadata_region
pipeline              connect-search-demo
```

這兩個轉換都只處理單筆 record。

它們不需要查詢其他事件。

---

# 設計問題 5：Partition

先定義 partition。

```text
partition = topic 內部的分段
```

同一個 topic 可以切成多個 partition：

```text
product.events
  partition 0
  partition 1
  partition 2
```

Kafka 會依照 key 或分配策略，把 event 放進其中一個 partition。

這讓資料可以被平行處理。

---

# 設計問題 6：Task

task 可以先理解成實際搬資料的 worker。

```text
task = connector 實際執行資料搬運工作的單位
```

Sink connector 會讓 task 從 Kafka partitions 讀資料。

Demo 設定：

```text
product.events topic: 3 partitions
Elasticsearch sink: tasks.max=2
```

`tasks.max=2` 表示最多允許 2 個 task；實際 task 數仍受 partitions、connector 行為與外部系統限制。

---

# 設計問題 7：DLQ 壞資料隔離區

DLQ 是 Dead Letter Queue。

它用來暫存無法處理的壞資料。

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

---

# Demo 的 DLQ 設定

Demo 設定：

```text
errors.tolerance=all
errors.deadletterqueue.topic.name=product.events.dlq
```

此 demo 驗證的是解析或轉換階段的壞資料會進入 DLQ。

---

# DLQ 之後仍要處理問題

DLQ 代表問題資料被隔離，後續仍要處理：

- 外部系統故障。
- 寫入 Elasticsearch 時欄位型別不相容。
- 下游長時間變慢。
- 監控、告警與補償流程。

---

# 設計問題 8：Kafka Connect 也需要記住狀態

Kafka Connect worker 需要記住三類資訊：

- config：connector 設定
- offset：資料讀到哪裡
- status：connector 與 task 是否正常

這些狀態在 distributed mode 會寫回 Kafka。

distributed mode 指的是多個 Kafka Connect worker 可以一起工作，因此狀態不能只放在單一 worker 本機。

---

# Internal Topics

internal topics 是 Kafka Connect 用來保存自身狀態的 Kafka topics。

```text
connect-configs-hot-product-demo
connect-offsets-hot-product-demo
connect-status-hot-product-demo
```

用途：

- configs topic：保存 connector 設定
- offsets topic：保存資料讀到哪裡
- status topic：保存 connector / task 狀態

所以 worker 重啟後，可以從 Kafka 取回設定、進度與狀態。

---

# 設計問題 9：資料會不會重複寫入？

資料管線可能會重送資料。

常見原因：

- worker 寫到一半失敗。
- 網路短暫中斷。
- 外部系統回應逾時。

因此要先理解 delivery semantics。

---

# Delivery Semantics

跨系統資料管線需要明確說明 delivery semantics。

```text
delivery semantics = 資料送出與重送時，系統能提供的處理保證
```

它回答三個問題：

- 是否可能漏資料？
- 是否可能重複？
- 重複時會不會造成業務錯誤？

---

# At-least-once 與 Exactly-once

先用白話理解：

```text
at-least-once = 至少送到一次，可能重複
exactly-once = 結果看起來只處理一次
```

此 demo 不宣稱跨 Kafka 到 Elasticsearch 的整條管線一定 exactly-once。

精確說法是：

```text
Elasticsearch sink 以 at-least-once 方式理解。
```

---

# 這個 demo 的重送處理

降低重送影響的方法：

```text
Kafka record key = event_id
Elasticsearch document id = key
write.method = upsert
```

upsert 的意思是：

```text
有就更新，沒有就新增
```

這是實務上的冪等處理：用穩定 id 降低重複寫入影響。

它不等於跨系統通用 exactly-once。

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
  提供事件搜尋與時間序列統計

Kibana
  讓人看到趨勢與問題
```

這個設計的目的，是分離不同系統責任。

---

# 本章重點

Kafka Connect 是標準化資料整合框架。

回到同一條主軸：

```text
資料方向、資料格式、錯誤隔離、狀態觀察、安全重送
```

這五件事決定一條資料管線能不能長期維運。

---

# 一句話總結

```text
折價券搶領爆量時，
正式狀態系統不應直接承擔搜尋與觀測壓力。

Kafka 先接住事件，
Kafka Connect 負責把事件穩定送到 Elasticsearch，
Kibana 讓團隊近即時觀察趨勢。
```

這就是這個 demo 要呈現的資料管線設計。

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

因此每次重播都能得到一致的 dashboard 結果。

E2E 驗證：

```bash
just e2e
```
