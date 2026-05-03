# 用熱門商品觀測 Demo 理解 Kafka Connect 資料管線設計

電商平台需要近即時觀察熱門商品的流量、失敗原因與地區壓力。這類需求看起來像 dashboard 問題，實際上會牽涉事件設計、資料管線、Kafka、Kafka Connect、Elasticsearch 與 Kibana 的分工。

這個 demo 的核心問題是：

```text
熱門商品事件如何進入 dashboard？
```

這個問題可以從業務 insight 開始，逐步推導到指標、事件欄位、資料管線與 Kafka Connect 的設計問題。

## 1. 從想得到的 Insight 開始

假設今晚 8 點有一個商品突然爆紅。營運與工程團隊需要快速判斷：

- 商品流量是否正在快速上升？
- 失敗原因是否集中在售罄、限流或付款失敗？
- 哪些地區的流量壓力最高？

這些判斷就是 dashboard 要提供的 insight。設計順序可以整理為：

```text
Insight -> 指標 -> 事件 -> 資料管線
```

在判斷 Kafka、Elasticsearch 與 Kibana 的分工之前，需要先把團隊想得到的判斷轉成可計算的指標。

## 2. 把 Insight 轉成指標

熱門商品爆紅時，團隊需要觀察的指標可以包含：

- 流量是否正在快速上升？
- 使用者是在瀏覽、點擊購買，還是一直重新整理？
- 購買或領券成功率是否下降？
- 失敗原因是售罄、限流，還是付款失敗？
- 哪些地區壓力最大？
- 是否有少數使用者出現高頻操作線索？

這些指標可以先分成兩類。

第一類是交易結果。交易系統通常關心訂單是否成立、庫存是否扣除、付款是否成功。這些資料代表正式業務狀態。

第二類是熱門商品狀態。這類指標需要分析事件流，例如每分鐘有多少瀏覽、點擊量是否突然上升、失敗事件是否集中在某個時間點、售罄後使用者是否仍大量重試。

這個 demo 聚焦第二類，也就是熱門商品狀態的近即時觀測。

## 3. 這個 demo 選定的觀測指標

這個 demo 關心「熱門商品或限量折價券是否正在爆量，以及爆量後發生什麼事」。可觀測指標整理如下：

| 指標 | 想回答的問題 |
| --- | --- |
| 事件總數 | 事件是否已進入查詢系統？ |
| 事件類型趨勢 | 流量是在瀏覽、重新整理、點擊、成功，還是失敗？ |
| 關鍵行為統計 | 成功、失敗與需求壓力事件各自累積多少？ |
| 失敗原因 | 是售罄、限流，還是付款失敗？ |
| 高頻操作線索 | 反覆重新整理或搶購失敗是否集中於少數使用者？ |
| 地區流量 | 哪些地區的壓力最高？ |

先定義指標，後續才能決定事件需要包含哪些欄位、資料管線要保存哪些資訊、dashboard 需要哪些 panel，以及資料管線是否真的支援觀測。

## 4. 指標由事件形成

Dashboard 上的指標通常由大量事件計算而來：

```text
單筆事件
  -> 依時間、類型、地區、商品分組
  -> 計算 count / rate / top N
  -> 形成 dashboard 指標
```

例如：

- 每分鐘瀏覽量 = 每分鐘 `PRODUCT_VIEWED` 事件數
- 失敗原因分布 = 依 `failure_reason` 分組後計數
- 地區流量 = 依 `metadata.region` 分組後計數

因此，指標會反推事件欄位。

在這個 demo 中，一筆 event 可以是：

- 使用者瀏覽商品
- 使用者重新整理頁面
- 使用者領券成功
- 使用者領券失敗

要支援前面的觀測指標，event 至少需要包含：

```text
event_type
occurred_at
user_id
product_id / coupon_id
remaining_stock / remaining_coupons
failure_reason
metadata.region
```

這些欄位和觀測指標的關係如下：

| 欄位 | 支援的指標 | 用途 |
| --- | --- | --- |
| `event_type` | 事件類型趨勢、關鍵行為統計 | 區分瀏覽、重新整理、排隊、成功、失敗等事件 |
| `occurred_at` | 事件類型趨勢、每分鐘流量 | 把事件放到時間軸上，才能做每分鐘或每段時間統計 |
| `user_id` | 高頻操作線索 | 找出是否有少數使用者大量重新整理頁面或重試 |
| `product_id / coupon_id` | 特定商品或折價券的觀測 | 確認 dashboard 觀察的是哪個商品或哪張折價券 |
| `remaining_stock / remaining_coupons` | 售罄壓力、成功與失敗比例 | 判斷失敗是否和庫存或券數耗盡有關 |
| `failure_reason` | 失敗原因 | 區分售罄、限流、付款失敗等失敗類型 |
| `metadata.region` | 地區流量 | 比較不同地區的流量壓力 |

Dashboard 想回答的問題，會直接決定 event model 的欄位設計。

## 5. Service 背後的工作分工

熱門商品觀測看起來是一個 dashboard，背後至少包含四種工作：

| 工作 | 例子 |
| --- | --- |
| 處理交易 | 建立訂單、扣庫存、付款 |
| 建立可查詢資料 | 把事件整理成適合搜尋與聚合的格式 |
| 查詢事件 | 查某段時間的瀏覽、重新整理、成功、失敗 |
| 聚合與統計 | 計算每分鐘流量、失敗原因、地區分布 |

處理交易時通常也會產生事件。差別在於用途：

| 輸出 | 用途 |
| --- | --- |
| 交易結果 | 保存正式業務狀態，例如訂單、付款、庫存 |
| 事件紀錄 | 送進資料管線，用於觀測、搜尋與統計 |

因此，交易流程可以同時寫入交易狀態並送出 event；後續架構設計要決定的是，這些 event 由誰保存、轉換、查詢與聚合。

架構設計要回答的問題是：

```text
這些工作要由同一個系統承擔，還是拆給不同系統負責？
```

這裡可以先比較三個候選方案。

## 6. 候選方案一：直接查交易 DB

交易 Database 適合承擔「處理交易」與「保存正式狀態」。例如訂單、付款、庫存、使用者資料。這些資料需要正確性與一致性。

如果同一個交易 DB 還要承擔「查詢事件」與「聚合統計」，熱門商品爆量時會出現幾個風險：

- 大量查詢可能影響交易系統。
- event-style 查詢和交易 DB 的主要設計目標不同。
- 每分鐘聚合、失敗原因統計、地區流量分析會和交易 workload 混在一起。
- 歷史事件查詢與稽核資料可能讓主資料庫膨脹。

設計判斷可以整理為：

```text
交易 DB 保存正式狀態。
事件查詢與聚合統計需要獨立評估負載、資料量與查詢型態。
```

這個判斷會自然導向第二個問題：事件查詢與聚合統計要放在哪裡處理？它們需要的能力和交易 DB 不同，更接近事件搜尋、時間序列統計與 dashboard 查詢。

## 7. 候選方案二：Application 直接寫 Elasticsearch

Elasticsearch 適合承擔三種工作：

- 建立可查詢資料
- 查詢事件
- 聚合與統計

在這個 demo 中，Elasticsearch index 可以想成「為事件查詢與 dashboard 準備好的資料表」。

因此，Elasticsearch 可以作為觀測查詢系統。新的問題是：事件要怎麼從 application 穩定進入 Elasticsearch？

如果電商 application 同步寫 Elasticsearch，使用者請求會變成：

```text
使用者請求
    |
    +-> 寫交易 DB
    |
    +-> 寫 Elasticsearch
```

這會讓 application 自行處理更多責任：

- 寫入搜尋系統可能變慢。
- 失敗後要重試。
- 下游變慢時，使用者請求可能被迫等待。
- 外部系統短暫失敗。
- 重送造成的重複寫入。

使用者請求會受到外部系統速度、重試與錯誤處理影響。因此，application 層可以保留兩個必要責任：

```text
產生 event
寫入一個可緩衝、可重放的地方
```

後面的搜尋、觀測與 dashboard 寫入，交給資料管線處理。

這個推導形成第三個候選方案：application 先把 event 寫入 Kafka，再由 Kafka Connect 把事件搬到 Elasticsearch。

## 8. Kafka 的角色：承接事件流

第一步先引入 Kafka：

```text
Application
    |
    | event
    v
Kafka
```

Kafka 在這個 demo 中扮演事件流入口。Application 把 event 送到 Kafka 後，不需要同步等待 dashboard 系統完成寫入。

Kafka 會把 event 放進 topic：

```text
topic = 一條有名字的事件流
```

這個 demo 的 topic 是：

```text
product.events
```

Kafka 提供兩個重要能力：

- buffering：後面的系統變慢時，事件仍可先保留在 Kafka。
- replay：後面的系統需要重建資料時，可以重新讀 topic。

因此，application 與觀測系統可以用 Kafka 解耦。Kafka 先保存事件，再由後續系統依各自速度消費。

講座 demo 主要使用限量折價券事件：

- `COUPON_VIEWED`
- `PAGE_REFRESHED`
- `WAITING_ROOM_JOINED`
- `COUPON_CLAIM_SUCCEEDED`
- `COUPON_CLAIM_FAILED`

這組事件涵蓋重新整理、排隊、售罄與失敗原因。

## 9. Kafka Connect 的角色：把 Kafka 接到外部系統

Kafka Connect 負責把 Kafka topic 接到外部系統。在這個 demo 中：

```text
Kafka topic: product.events
        |
        v
Kafka Connect
        |
        v
Elasticsearch index: product-events
```

可以先用一句話理解：

```text
Kafka Connect = Kafka 與外部系統之間的標準資料搬運層
```

這個 demo 的完整架構如下：

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

這條管線的目標包含資料流動、可觀察性、可維護性、可重跑能力與錯誤處理。

## 10. 執行 Demo 後會看到什麼

Demo 會產生一組固定劇本的折價券搶購流量。Dashboard 可以看到：

- 事件總數快速累積。
- 事件類型隨時間變化。
- 成功與失敗比例改變。
- 售罄或限流造成失敗原因集中。
- 高頻操作線索浮現。
- 不同地區有不同流量壓力。

啟動 stack：

```bash
just setup
```

產生一組固定劇本的流量：

```bash
just replay-demo
```

Dashboard：

```text
http://localhost:5601/app/dashboards#/view/hot-product-sales-dashboard
```

這個 demo 採用可重播模式：

- 每次先清理上一輪 demo 狀態。
- 使用固定 `BASE_TIME=2026-05-01T12:00:00Z`。
- 重新產生 24,000 筆折價券搶購事件。
- Dashboard time range 會對齊固定事件時間窗。

因此，每次執行都能得到一致的 dashboard 結果。

## 11. Demo Panel 與健康狀態

目前 dashboard 觀察項目如下：

| Panel | 目的 |
| --- | --- |
| 事件總數 | 確認事件已進入查詢系統 |
| 事件類型趨勢 | 看瀏覽、重新整理、成功、失敗如何隨時間變化 |
| 關鍵行為統計 | 看成功、失敗、需求壓力事件的累積數量 |
| 失敗原因 | 看售罄、限流、付款失敗是否集中 |
| 高頻操作線索 | 觀察反覆重新整理或搶購失敗是否集中於少數使用者 |
| 地區流量 | 比較不同地區的壓力分布 |

`關鍵行為統計` 是依條件分組後的事件數。完整轉換率需要額外定義漏斗分母與分子。

資料管線是否健康，還需要搭配：

- connector / task 狀態
- Elasticsearch 寫入結果
- 端到端檢查

## 12. 回到 Kafka Connect 第四章

Demo 建立直覺後，再回到 Kafka Connect 第四章。前面已經建立這條資料流：

```text
event -> topic -> Kafka -> Kafka Connect -> Elasticsearch
```

第四章可以從一個基本問題展開：

```text
這條資料管線跑不跑得起來？
```

接著延伸成幾個工程面向：

- 資料往哪裡走？
- 資料怎麼被解析與轉換？
- 壞資料去哪裡？
- 出事時如何觀察？
- 資料重送時是否會造成業務錯誤？

下面用 demo 對應這些設計問題。

### 12.1 Connector：資料方向

```text
connector = Kafka Connect 用來連接外部系統的元件
```

Connector 先看資料方向：

```text
Source connector:
  外部系統 -> Kafka

Sink connector:
  Kafka -> 外部系統
```

這個 demo 是 sink：

```text
Kafka -> Elasticsearch
```

原因是要把 Kafka events 變成可以搜尋、聚合與視覺化的資料。

### 12.2 Event Model：Dashboard 要回答什麼問題

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

Event model 屬於資料設計問題，會影響 converter、SMT、sink connector 與 dashboard 的後續處理。

### 12.3 Converter：把 bytes 解析成 record

Kafka 裡的資料本質上是 bytes。Kafka Connect 要先把資料解析成有欄位的 record，後面的 SMT 與 sink 才知道怎麼處理。

```text
ConnectRecord = Kafka Connect 內部用來表示一筆資料的標準格式
converter = Kafka bytes 與 ConnectRecord 之間的轉換器
```

Sink pipeline 中的流程：

```text
Kafka bytes
    |
    | JsonConverter
    v
ConnectRecord
```

此 demo 使用 schemaless JSON，也就是不附帶 schema 定義的 JSON。這對初學者容易觀察，Kibana 也能直接看到欄位。Production 環境通常還需要明確的 schema 與相容性規則。

### 12.4 SMT：對單筆 record 做輕量轉換

SMT 是 Single Message Transform。

```text
SMT = 對單筆 record 做輕量轉換
```

SMT 只看當下這一筆資料，適合加欄位、改欄位名、刪欄位或展平欄位。SMT 不會拿多筆事件一起計算，也不會查其他資料表。

這個 demo 使用兩個 SMT：

```text
Flatten:
  metadata.region -> metadata_region

InsertField:
  pipeline=connect-search-demo
```

這兩個轉換都只處理單筆 record。

### 12.5 Partition 與 Task：平行處理

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

Kafka 會依照 key 或分配策略，把 event 放進其中一個 partition。這讓資料可以被平行處理。

Task 可以先理解成實際搬資料的 worker。

```text
task = connector 實際執行資料搬運工作的單位
```

Demo 設定：

```text
product.events topic: 3 partitions
Elasticsearch sink: tasks.max=2
```

`tasks.max=2` 表示最多允許 2 個 task；實際 task 數仍受 partitions、connector 行為與外部系統限制。

### 12.6 DLQ：壞資料隔離區

DLQ 是 Dead Letter Queue，用來暫存無法處理的壞資料。

例如 malformed JSON：

```json
{"event_id":"bad_1",
```

設計問題是：

```text
整條資料管線停止？
保存壞資料，讓主流程繼續？
```

Demo 設定：

```text
errors.tolerance=all
errors.deadletterqueue.topic.name=product.events.dlq
```

此 demo 驗證的是解析或轉換階段的壞資料會進入 DLQ。

DLQ 代表問題資料被隔離。後續仍要處理外部系統故障、寫入 Elasticsearch 時欄位型別不相容、下游長時間變慢，以及監控、告警與補償流程。

### 12.7 Internal Topics：Kafka Connect 也需要記住狀態

Kafka Connect worker 需要記住三類資訊：

- config：connector 設定
- offset：資料讀到哪裡
- status：connector 與 task 是否正常

Distributed mode 指的是多個 Kafka Connect worker 可以一起工作，因此狀態不能只放在單一 worker 本機。Kafka Connect 會用 internal topics 保存自身狀態：

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

### 12.8 Delivery Semantics：資料會不會重複寫入

資料管線可能會重送資料。常見原因包含 worker 寫到一半失敗、網路短暫中斷、外部系統回應逾時。

```text
delivery semantics = 資料送出與重送時，系統能提供的處理保證
```

它回答三個問題：

- 是否可能漏資料？
- 是否可能重複？
- 重複時會不會造成業務錯誤？

先用白話理解：

```text
at-least-once = 至少送到一次，可能重複
exactly-once = 結果看起來只處理一次
```

此 demo 不宣稱跨 Kafka 到 Elasticsearch 的整條管線一定 exactly-once。精確說法是：

```text
Elasticsearch sink 以 at-least-once 方式理解。
```

降低重送影響的方法：

```text
Kafka record key = event_id
Elasticsearch document id = key
write.method = upsert
```

`upsert` 的意思是「有就更新，沒有就新增」。這是實務上的冪等處理：用穩定 id 降低重複寫入影響。它不等於跨系統通用 exactly-once。

## 13. 本章重點

Kafka Connect 是標準化資料整合框架。這一章的重點可以收斂成四個設計問題：

- 資料往哪裡走？
- 資料怎麼被解析與轉換？
- 失敗資料怎麼處理？
- 狀態與重送怎麼理解？

這個 demo 用熱門商品觀測情境，把這四個問題串在一起：

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
