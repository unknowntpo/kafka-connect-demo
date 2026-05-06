# 限量折價券搶購活動，能即時看見趨勢嗎？

這一章用一個限量折價券搶購活動來介紹 Kafka Connect。

情境很接近日常電商活動：平台推出限量折價券，開放時間一到，大量使用者同時瀏覽活動頁、重新整理、進入等候室、嘗試領券。營運團隊想知道活動是否正在爆量，工程團隊想知道資料管線是否正常，客服或值班人員也需要判斷失敗原因是售罄、限流，還是資料格式出了問題。

這個 demo 的主問題是：

```text
折價券搶購事件已經發生，系統能不能把它們快速變成可查詢、可觀察的 dashboard？
```

整段 demo 會從 dashboard 開始，再沿著資料路線往回追到 Kafka、Kafka Connect connector config，以及 Dead Letter Queue。

## 1. 先用活動現場記住四個角色

學生第一次接觸 Kafka、Kafka Connect、Elasticsearch 與 Kibana 時，不需要先背產品名。可以先記住它們在活動現場扮演的工作：

| 工具 | 活動現場比喻 | 在 demo 裡的工作 |
| --- | --- | --- |
| Kafka | 等候區 | 先接住事件，讓後面的系統依自己的速度處理 |
| Kafka Connect | 搬運工 | 從 Kafka 讀資料，做輕量整理，寫入外部系統 |
| Elasticsearch | 可查詢倉庫 | 保存事件文件，支援搜尋、分組與聚合 |
| Kibana | 觀察台 | 把 Elasticsearch 文件畫成圖表與明細 |

資料路線可以先看成：

```text
+-----------+   +----------------+   +-------------------------+   +----------------+   +-------------+
| Generator |-->| Kafka 等候區   |-->| Kafka Connect 搬運工   |-->| ES 查詢倉庫    |-->| Kibana 觀察台|
| 產生事件  |   | product.events |   | 讀 Kafka / 整理 / 寫 ES |   | product-events |   | 圖表 + 明細  |
+-----------+   +----------------+   | JSON + SMT + upsert     |   +----------------+   +-------------+
                                     +------------+------------+
                                                  |
                                                  | source 訊息壞掉
                                                  | 搬運工寫入 Dead Letter Queue topic
                                                  v
                  +--------------------+   +-------------------------+   +--------------------+   +-------------+
                  | Kafka 等候區       |-->| Kafka Connect 搬運工   |-->| ES 查詢倉庫        |-->| Kibana 觀察台|
                  | product.events.dlq |   | 讀 DLQ / 保 raw / 補來源|   | product-events-dlq |   | count + raw  |
                  +--------------------+   +-------------------------+   +--------------------+   +-------------+
```

這張圖有兩條路線。

第一條是正常事件路線：折價券搶購事件進入 `product.events`，Kafka Connect Elasticsearch Sink 讀取這個 topic，將資料寫入 `product-events` index，Kibana 再從 Elasticsearch 查詢圖表與事件明細。

第二條是壞資料隔離路線：如果主 sink 在讀取 source 訊息時發現 malformed JSON，Kafka Connect 會把 raw bad record 寫入 `product.events.dlq`。另一條 Kafka Connect sink 再讀取這個 Dead Letter Queue topic，把 raw record 與來源資訊寫入 `product-events-dlq`，讓 Kibana 顯示壞資料數量與 raw document。

## 2. Demo 的第一個畫面：先建立張力

正式 demo 可以先打開 Kibana dashboard。第一眼要讓學生看到問題：

```text
24,000 筆折價券搶購事件湧入，我們能不能看見即時趨勢？
```

Dashboard 上的主要 panel 包含：

| Panel | 用途 |
| --- | --- |
| Demo 導覽 - 事件旅行路線 | 用四個角色建立資料路線地圖 |
| 事件明細 - Elasticsearch 文件 | 查看寫入 Elasticsearch 後的一筆筆 document |
| 熱門商品 - 事件總數 | 確認資料已進入查詢系統 |
| Kafka Connect - DLQ 壞資料數量 | 確認資料管線是否有壞 record 被隔離 |
| DLQ 明細 - Elasticsearch raw doc | 查看被隔離的 raw bad record 與來源 metadata |
| 熱門商品 - 事件類型趨勢 | 觀察瀏覽、重新整理、排隊、成功、失敗如何變化 |
| 熱門商品行為統計 | 比較五種事件類型各自累積多少 |
| 熱門商品 - 剩餘折價券變化 | 觀察折價券何時接近售罄、何時歸零 |
| 熱門商品 - 失敗原因 | 看失敗集中在售罄、限流或其他原因 |
| 熱門商品 - 高頻操作線索 | 找出反覆重新整理或多次失敗的使用者 |
| 熱門商品 - 地區流量 | 比較不同地區的流量壓力 |

Dashboard 預設 refresh interval 是 5 秒。`just run-demo` 會把 dashboard time range 設成本輪 demo 的 30 秒事件時間窗。Kibana 會每 5 秒重新查詢 Elasticsearch；它是定時重新查詢，並非瀏覽器和 Elasticsearch 之間的即時推送連線。

`just run-demo` 會讓事件從執行當下開始，並在 30 秒後結束。profile generator 會依事件的 `occurred_at` 節奏送出，讓 Kafka、Kafka Connect、Elasticsearch 與 Kibana 在現場看起來像一條正在流動的資料管線。這樣 demo 的時間軸更短，折價券下降、失敗集中與流量尖峰會更快出現在同一個畫面。

## 3. 第一次互動：執行 `just run-demo`

Demo 可以從一個乾淨狀態開始：

```bash
just setup
just run-demo
```

`just run-demo` 會執行一組固定劇本：

- 清理上一輪 demo 狀態。
- 建立 topic、Elasticsearch index、Kibana dashboard。
- 註冊主 Elasticsearch sink 與 DLQ sink。
- 產生 24,000 筆限量折價券事件。
- 驗證 `product-events` 已寫入 24,000 筆文件。
- 驗證 `product-events-dlq` 一開始是 0 筆。
- 檢查兩條 connector 都是 `RUNNING`。

這裡要讓學生先建立一個觀念：application 只要把事件送到 Kafka，後面由 Kafka Connect 搬到 Elasticsearch，Kibana 才能把文件畫成 dashboard。

## 4. 指標由事件形成

Dashboard 上的數字來自大量事件的聚合：

```text
單筆事件
  -> 依時間、類型、地區、使用者、失敗原因分組
  -> 計算 count / top N / time series
  -> 形成 dashboard 指標
```

這個 demo 產生五種主要事件：

```text
COUPON_VIEWED
PAGE_REFRESHED
WAITING_ROOM_JOINED
COUPON_CLAIM_SUCCEEDED
COUPON_CLAIM_FAILED
```

為了支援 dashboard，event 至少需要包含：

| 欄位 | 例子 | 支援的觀測 |
| --- | --- | --- |
| `event_id` | `evt_b5b614...` | 對齊 Kafka record key 與 Elasticsearch `_id` |
| `event_type` | `PAGE_REFRESHED` | 事件類型趨勢、行為統計 |
| `occurred_at` | `2026-05-05T21:19:20.967Z` | 時間序列 |
| `user_id` | `user_09230` | 高頻操作線索 |
| `coupon_id` | `coupon_mayday_001` | 指定折價券活動 |
| `remaining_coupons` | `30` | 剩餘折價券變化 |
| `failure_reason` | `COUPON_SOLD_OUT` | 失敗原因 |
| `metadata.region` | `us-east-1` | 地區流量 |

目前事件模型刻意只保留一個對外庫存觀測欄位：`remaining_coupons`。generator 內部仍會用扣減前後的數值判斷領券成功、失敗與售罄，但 Kafka event 只輸出「這筆事件發生後，折價券還剩幾張」。這樣學生在事件明細裡不需要同時分辨 `remaining_stock`、`inventory_before`、`inventory_after` 這些相近欄位。

## 5. 正式系統中，誰寫出這些 event？

在 demo 裡，`Generator` 扮演活動系統，負責產生事件。

在正式系統裡，事件通常由處理領券流程的 application service 或 domain service 寫出。它會先完成必要的業務判斷，例如使用者資格、限流、折價券是否還有剩餘數量、領券紀錄是否成立，然後送出事件供觀測系統使用。

可以把責任拆成：

| 責任 | 通常由誰處理 |
| --- | --- |
| 判斷使用者能不能領券 | application service / domain service |
| 扣減正式折價券數量 | application service 搭配正式狀態 DB、交易或一致性機制 |
| 寫出觀測事件 | application service、outbox publisher 或事件發佈元件 |
| 建立搜尋與 dashboard 用索引 | Kafka Connect sink |
| 查詢與視覺化 | Elasticsearch / Kibana |

Kafka Streams 可以用來維護衍生狀態、統計、投影或額外的 stream processing。若要讓 Kafka Streams 成為扣庫存的權威路徑，系統必須明確設計 command、state store、交易邊界與一致性模型。這個 demo 的目的較窄：觀察已經發生的活動事件，因此正式扣減邏輯放在事件產生端理解，Kafka Connect 負責把事件搬到查詢系統。

## 6. Kafka：等候區與可重放事件流

Kafka 在這個 demo 中扮演事件流入口。

```text
Application / Generator
        |
        | event
        v
Kafka topic: product.events
```

`topic` 可以先理解成一條有名字的事件流。`product.events` 保存限量折價券活動事件，讓後面的系統可以依自己的速度讀取。

Kafka 在這裡提供三個能力：

- buffering：下游變慢時，事件仍先留在 Kafka。
- producer retry：producer 遇到暫時性送出失敗時，可以在送達期限內重試；demo 另外設定 `acks=all`，要求 broker 確認寫入。
- replay：需要重建 Elasticsearch index 時，可以重新讀 topic。

Kafka record key 在這個 demo 中設定為 `event_id`。generator 產生 event 後，用同一個 `event_id` 當 Kafka record key：

```text
Kafka record key = event_id
Kafka record value = JSON event
```

event id 由 `UUID.nameUUIDFromBytes(...)` 依照 scenario、event type、sequence 與 seed 產生。這是 deterministic id；同一組 profile 與 seed 重新產生時，對應事件會得到相同 id。它長得像 UUID，用途是穩定事件識別；每次重新產生同一段劇本時，不會隨機換成新的 id。

## 7. Kafka Connect：搬運工與 connector pipeline

Kafka Connect 負責把 Kafka topic 接到外部系統。這個 demo 使用 Elasticsearch Sink Connector：

```text
Kafka topic: product.events
        |
        v
Kafka Connect Elasticsearch Sink
        |
        v
Elasticsearch index: product-events
```

可以用「connector pipeline」這個口語說法描述整條搬運流程，但在 Kafka Connect 的正式術語裡，主要物件是：

| 術語 | 意義 |
| --- | --- |
| worker | 執行 Kafka Connect runtime 的程序 |
| connector | 定義資料來源、目的地與設定的元件 |
| task | 實際搬資料的執行單位 |
| converter | Kafka bytes 和 Connect record 之間的轉換器 |
| SMT | Single Message Transform，對單筆 record 做輕量整理 |

這個 demo 的主 connector config 位於：

```text
connectors/elasticsearch-sink-product-events.json
```

主 sink 使用：

```json
{
  "topics": "product.events",
  "topic.to.external.resource.mapping": "product.events:product-events",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "key.ignore": "false",
  "write.method": "upsert"
}
```

這段設定說明三件事：

- 從 `product.events` 讀資料。
- 寫到 Elasticsearch index `product-events`。
- 用 Kafka record key 當 Elasticsearch document id，並用 `upsert` 寫入。

## 8. Converter：把 Kafka bytes 變成 Connect record

Kafka 裡的資料本質上是 bytes。Kafka Connect 要先透過 converter 把 bytes 解析成 Connect record，後面的 SMT 與 sink connector 才能處理欄位。

主 sink 設定：

```json
{
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false"
}
```

這代表 value 是 schemaless JSON。正常事件會被解析成帶欄位的 Connect record，後面才能進行 `Flatten`、`InsertField` 與 Elasticsearch 寫入。

如果 value 是 malformed JSON，解析階段就會失敗。這正是 Dead Letter Queue demo 要展示的錯誤路徑。

## 9. SMT：搬運途中的兩個輕量調整

SMT 是 Single Message Transform，意思是「每一筆 record 寫到外部系統前，先做一點輕量整理」。

SMT 有兩個限制要先講清楚：

- SMT 一次只處理一筆 record。
- SMT 適合改欄位、加欄位、刪欄位、展平欄位；跨多筆資料的統計、join、去重複，需要 Kafka Streams、Flink、資料庫或下游系統處理。

這個 demo 設定：

```json
{
  "transforms": "flattenMetadata,addPipeline",
  "transforms.flattenMetadata.type": "org.apache.kafka.connect.transforms.Flatten$Value",
  "transforms.flattenMetadata.delimiter": "_",
  "transforms.addPipeline.type": "org.apache.kafka.connect.transforms.InsertField$Value",
  "transforms.addPipeline.static.field": "pipeline",
  "transforms.addPipeline.static.value": "connect-search-demo"
}
```

`Flatten` 和 `InsertField` 是 Kafka Connect 提供的 SMT 類型。`flattenMetadata` 與 `addPipeline` 是這份 connector config 裡替兩個 transform 取的名字。

第一個 transform 是 `flattenMetadata`，它使用 `Flatten$Value`：

```text
metadata.region -> metadata_region
```

Kafka raw event：

```json
{
  "event_id": "evt_001",
  "metadata": {
    "region": "TW-NORTH"
  }
}
```

寫入 Elasticsearch 前會變成：

```json
{
  "event_id": "evt_001",
  "metadata_region": "TW-NORTH"
}
```

因為 delimiter 設成 `_`，所以 `metadata.region` 會變成 `metadata_region`。這讓 Kibana 可以直接用一層欄位做 terms aggregation。

第二個 transform 是 `addPipeline`，它使用 `InsertField$Value`：

```json
{
  "pipeline": "connect-search-demo"
}
```

這個欄位是來源標記。打開 Kibana 的事件明細時，可以確認這筆 document 是由這條 Kafka Connect pipeline 寫入。

## 10. Elasticsearch：可查詢倉庫與 upsert

Elasticsearch 在 demo 中保存兩個 index：

| Index | 用途 |
| --- | --- |
| `product-events` | 正常事件 document |
| `product-events-dlq` | Dead Letter Queue raw bad record document |

主 sink 使用：

```json
{
  "key.ignore": "false",
  "write.method": "upsert"
}
```

`key.ignore=false` 表示 Elasticsearch Sink 會使用 Kafka record key。因為 demo 的 Kafka record key 等於 `event_id`，所以 Elasticsearch document `_id` 也會等於 `event_id`。

`upsert` 的意思是：

```text
有同一個 _id 的 document -> 更新
沒有同一個 _id 的 document -> 新增
```

它和 Kafka Connect 的 delivery semantics 搭配使用。

Kafka Connect sink 通常以 at-least-once 方式理解。發生重試、task restart、offset commit 時機差異時，同一筆 Kafka record 可能被送到 Elasticsearch 超過一次。

如果每次重送都建立新 document，dashboard 計數會膨脹。demo 透過穩定 id 降低這個問題：

```text
同一筆事件重送
        |
        v
Kafka record key 相同
        |
        v
Elasticsearch _id 相同
        |
        v
upsert 更新同一份 document
```

這是實務上的冪等寫入設計。它降低重複寫入對 dashboard 的影響，但不代表跨 Kafka 到 Elasticsearch 的整條管線提供通用 exactly-once 保證。

可以對學生這樣說：

```text
at-least-once 解決資料盡量不要漏掉；
upsert 解決同一筆事件重送時不要變成多份文件。
```

## 11. Dead Letter Queue：壞資料隔離區

Dead Letter Queue 用來保存 Kafka Connect 無法處理的壞資料。這個 demo 主要展示 malformed JSON：

```json
{
  "event_id": "evt_bad_json_001",
  "event_type": "COUPON_VIEWED",
  "occurred_at": "2026-05-05T12:00:00Z"
```

這筆資料故意少最後一個右大括號。主 Elasticsearch sink 使用 `JsonConverter` 解析 value 時會失敗。

主 sink 設定：

```json
{
  "errors.tolerance": "all",
  "errors.deadletterqueue.topic.name": "product.events.dlq",
  "errors.deadletterqueue.context.headers.enable": "true",
  "errors.log.enable": "true",
  "errors.log.include.messages": "true"
}
```

`errors.tolerance=all` 表示 connector 可以容忍可被錯誤處理機制接住的 record-level error，並繼續處理其他正常 record。`errors.deadletterqueue.topic.name=product.events.dlq` 指定壞資料要寫入哪個 topic。

若沒有設定 `errors.tolerance=all`，Kafka Connect 預設是：

```json
{
  "errors.tolerance": "none"
}
```

在這種情況下，主 sink 讀到 malformed JSON 後會讓 task 進入 `FAILED`。這筆 bad record 不會被寫進 `product.events.dlq`，task 重啟後通常仍會在同一個 offset 再次遇到同一筆壞資料。

可以用活動現場比喻：

```text
沒有錯誤隔離設定：搬運工遇到一箱壞掉的貨就停工。
有 Dead Letter Queue：搬運工把壞貨放到錯誤隔離區，其他正常貨繼續搬。
```

## 12. DLQ Sink：讓 Kibana 也看得到壞資料

Kafka Connect 把壞資料寫到 `product.events.dlq` 後，資料仍在 Kafka topic 裡。Kibana 無法直接查 Kafka topic，因此 demo 另外建立第二條 sink：

```text
Kafka topic: product.events.dlq
        |
        v
Kafka Connect DLQ Sink
        |
        v
Elasticsearch index: product-events-dlq
```

DLQ sink config 位於：

```text
connectors/elasticsearch-sink-product-events-dlq.json
```

它使用 `StringConverter` 讀 raw bad record：

```json
{
  "value.converter": "org.apache.kafka.connect.storage.StringConverter"
}
```

接著用兩個 SMT 整理成可查詢文件：

```json
{
  "transforms": "hoistRaw,addDlqContext",
  "transforms.hoistRaw.type": "org.apache.kafka.connect.transforms.HoistField$Value",
  "transforms.hoistRaw.field": "raw_record",
  "transforms.addDlqContext.type": "org.apache.kafka.connect.transforms.InsertField$Value",
  "transforms.addDlqContext.static.field": "pipeline",
  "transforms.addDlqContext.static.value": "connect-search-demo-dlq",
  "transforms.addDlqContext.topic.field": "source_topic",
  "transforms.addDlqContext.partition.field": "source_partition",
  "transforms.addDlqContext.offset.field": "source_offset",
  "transforms.addDlqContext.timestamp.field": "dlq_timestamp"
}
```

`HoistField` 把 raw value 包成：

```json
{
  "raw_record": "{...malformed json..."
}
```

`InsertField` 補上來源追查欄位，例如 `source_topic`、`source_partition`、`source_offset`、`dlq_timestamp` 與 `pipeline`。

因此，Kibana 會有兩個 DLQ 相關 panel：

| Panel | 目的 |
| --- | --- |
| Kafka Connect - DLQ 壞資料數量 | 顯示 `product-events-dlq` 的 document count |
| DLQ 明細 - Elasticsearch raw doc | 展開查看 `raw_record` 與來源 metadata |

正常 demo 一開始 DLQ count 應為 0。手動送 malformed JSON 後，這個數字應該變成 1，raw doc panel 也應該看得到該筆壞資料。

## 13. 第二次互動：手動送一筆壞資料

`just run-demo` 跑完後，可以用 Redpanda Console 手動送一筆 malformed JSON 到 `product.events`。

key：

```text
evt_bad_json_001
```

value 故意少最後一個 `}`：

```json
{
  "event_id": "evt_bad_json_001",
  "event_type": "COUPON_VIEWED",
  "occurred_at": "2026-05-05T12:00:00Z"
```

接著觀察三個地方：

1. Redpanda Console 的 `product.events.dlq` topic 出現 record。
2. Kibana 的「Kafka Connect - DLQ 壞資料數量」從 0 變 1。
3. Kibana 的「DLQ 明細 - Elasticsearch raw doc」可以展開查看 `raw_record`。

也可以用 Elasticsearch API 驗證：

```bash
curl -s http://localhost:9200/product-events-dlq/_count
```

這個互動能讓學生看到：Dead Letter Queue 是 Kafka Connect 遇到壞 record 時額外寫出的一條錯誤隔離路線。

## 14. Dashboard 從哪裡讀資料

Kibana dashboard 的資料來源是 Elasticsearch data view。

正常事件 panel 讀：

```text
index: product-events
time field: occurred_at
```

DLQ panel 讀：

```text
index: product-events-dlq
```

DLQ data view 在這個 demo 中不設定 time field。原因是正常情況下 DLQ 應為 0 筆；若 DLQ panel 受到 dashboard time range 限制，學生可能會誤以為壞資料消失。這裡讓 DLQ count 直接反映 `product-events-dlq` 的文件數。

Dashboard saved object 設定：

```text
refresh interval: 5 seconds
time range: demo start to demo start + 30s
```

如果在 Kibana edit mode 中看到舊版 layout 或 panel 消失，可能是瀏覽器保留了 unsaved changes。現場 demo 應先確認是否要 drop unsaved changes，再重新整理 dashboard。

## 15. Kafka Connect 能不能做去重複？

Kafka Connect 的定位是資料整合與搬運。它沒有像 Kafka Streams 那樣的 state store，也不適合在 connector 裡做跨 record 的去重複邏輯。

SMT 只能看單筆 record，因此無法回答：

```text
這個 event_id 之前有沒有出現過？
```

如果 Kafka 裡有重複 record，常見處理方式是：

| 做法 | 適用情境 |
| --- | --- |
| 穩定 key + Elasticsearch upsert | 同一事件重送時 id 相同，降低重複文件 |
| Kafka Streams / Flink stateful dedupe | 需要在時間窗內記住已看過的 id |
| 正式 DB unique constraint | 業務事實需要唯一性保證 |
| 下游查詢去重 | dashboard 或分析查詢可以接受查詢時計算 |

這個 demo 採用第一種：`event_id` 當 Kafka record key，Elasticsearch 用同一個 `_id` 做 `upsert`。它處理的是「同一筆事件被重送」的影響。若 producer 真的產生兩筆不同 `event_id` 但語意重複的事件，就需要上游業務邏輯或 stateful stream processing 處理。

## 16. At-least-once、upsert 與資料正確性

這個 demo 可以用三句話介紹 delivery semantics：

```text
Kafka Connect sink 通常要用 at-least-once 思考。
at-least-once 表示資料盡量送到，代價是可能重送。
upsert 使用穩定 document id，降低重送造成的重複文件。
```

對照到設定：

```json
{
  "key.ignore": "false",
  "write.method": "upsert"
}
```

對照到資料：

```text
Kafka record key = evt_b5b614...
Elasticsearch _id = evt_b5b614...
event_id = evt_b5b614...
```

第一次送到 Elasticsearch：

```text
_id=evt_b5b614... 不存在 -> 建立 document
```

重試再送一次：

```text
_id=evt_b5b614... 已存在 -> 更新同一份 document
```

這讓 demo 的 dashboard 在 connector retry 或 restart 後不容易因同一筆事件重送而多算。

## 17. Internal Topics：Kafka Connect 自己也需要狀態

Kafka Connect worker 需要保存三種狀態：

| 狀態 | 用途 |
| --- | --- |
| config | connector 設定 |
| offset | 每個 task 讀到哪裡 |
| status | connector / task 是否正常 |

Distributed mode 下，這些狀態會保存在 Kafka internal topics，例如：

```text
connect-configs-hot-product-demo
connect-offsets-hot-product-demo
connect-status-hot-product-demo
```

這讓 worker 重啟後可以取回 connector 設定、讀取進度與狀態。這些 internal topics 是 Kafka Connect runtime 的狀態保存機制；它和 Kafka Streams 用來做業務運算的 state store 不同。

## 18. 現場講解順序建議

這個 demo 可以用「起承轉合」安排：

### 起：看見問題

先打開 dashboard，讓學生看到 24,000 筆折價券搶購事件、事件類型趨勢、剩餘折價券下降與失敗原因集中。

可以問：

```text
如果活動正在爆量，團隊要怎麼知道現在發生什麼事？
```

### 承：沿著資料路線追

從 dashboard 的「事件明細 - Elasticsearch 文件」找一筆 event，觀察 `event_id`、`event_type`、`remaining_coupons`、`metadata_region` 與 `pipeline`。

再到 Redpanda Console 找 `product.events` 裡的 raw record，對照：

```text
Kafka raw record: metadata.region
Elasticsearch document: metadata_region
```

這裡帶出 Converter 與 SMT。

### 轉：故意製造壞資料

手動送 malformed JSON 到 `product.events`。

觀察：

```text
product.events.dlq topic
product-events-dlq index
DLQ count panel
DLQ raw doc panel
```

這裡帶出 `errors.tolerance=all`、Dead Letter Queue 與第二條 DLQ sink。

### 合：回到工程設計

最後回到 connector config，收斂成五個 Kafka Connect 設計問題：

| 問題 | demo 對應 |
| --- | --- |
| 資料方向 | sink connector：Kafka -> Elasticsearch |
| 資料格式 | JsonConverter 解析 schemaless JSON |
| 輕量轉換 | Flatten 與 InsertField SMT |
| 錯誤隔離 | Dead Letter Queue |
| 安全重送 | stable key + Elasticsearch upsert |

## 19. 有獎徵答

正式 demo 只需要一題，答案必須能在畫面上確認。

題目：

```text
Kafka Connect 讀到 malformed JSON 這類壞資料時，會把它放到哪個錯誤隔離區？
```

標準答案：

```text
DLQ / Dead Letter Queue
```

現場驗證：

```text
Kafka topic: product.events.dlq
Kibana panel: Kafka Connect - DLQ 壞資料數量
Kibana panel: DLQ 明細 - Elasticsearch raw doc
```

## 20. 本章重點

這個 demo 用限量折價券搶購活動，把 Kafka Connect 的核心概念串起來：

```text
Kafka
  等候區，先接住事件

Kafka Connect
  搬運工，讀 Kafka、整理欄位、寫 Elasticsearch

Elasticsearch
  可查詢倉庫，保存事件 document

Kibana
  觀察台，把 document 畫成趨勢、統計與明細

Dead Letter Queue
  壞資料隔離區，讓主流程繼續處理正常事件
```

這條路線背後的工程重點是：事件欄位要支援 dashboard 問題，connector config 要清楚定義方向、格式、轉換、錯誤隔離與重送策略。Demo 中的 `remaining_coupons`、`metadata_region`、`pipeline`、`event_id`、`key.ignore=false`、`write.method=upsert` 與 Dead Letter Queue，都是為了讓資料可以被觀察、追查與安全重送。
