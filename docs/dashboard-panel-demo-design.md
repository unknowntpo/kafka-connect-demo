# Dashboard Panel Demo 設計稿

這份文件用來設計 live demo 的講解順序。範圍限制在 dashboard panel、Kibana、Redpanda Console 與 Kafka Connect 的互動講解；暫不設計 drilldown。

目標是讓學生先看懂 dashboard 上的現象，再沿著資料路線往前追：

```text
+-------------------+     +----------------------+        Kafka Connect         +----------------------+     +------------------+
| Java Event        | --> | Kafka topic          | ---- Elasticsearch Sink --> | Elasticsearch        | --> | Kibana Dashboard |
| Generator         |     | product.events       |      讀 Kafka、寫 ES         | index: product-events|     |                  |
+-------------------+     +----------------------+                              +----------------------+     +------------------+
     產生事件                    先接住事件                         標準化搬運資料                      快速查詢                      畫成圖表
```

## 教學主軸

整場 demo 先從 dashboard 現象開始，再一路追到 Kafka 與 Kafka Connect。建議順序是：

1. 先看 dashboard 現象。
2. 再看 Elasticsearch 裡的一筆筆 document。
3. 再看 Redpanda Console 裡 Kafka topic 的 raw record。
4. 最後看 Kafka Connect config，說明中間如何解析、轉換與寫入。

每次切換工具前，先回到同一句話：

```text
我們現在沿著同一筆事件，從 dashboard 往資料源頭追一站。
```

這樣可以避免學生覺得 Kibana、Redpanda Console 與 Kafka Connect 是三個互不相關的工具。

## 建議畫面配置

正式 demo 建議使用三個固定畫面，不增加其他工具：

| 畫面 | 用途 | 教學角色 |
|---|---|---|
| Kibana Dashboard | 看圖表與事件明細 | 觀察結果 |
| Kibana Discover / saved search panel | 看 Elasticsearch document | 圖表背後的資料 |
| Redpanda Console | 看 Kafka topic 的 raw record | 資料進 Kafka 時的樣子 |

Kafka Connect REST API 與 connector config 只在需要時展示。它的角色是解釋「中間誰負責搬資料」，dashboard 仍然是 demo 主線。

## Panel 講解總表

| Panel | 它回答的問題 | 需要的欄位 | 帶出的 Kafka Connect 知識點 |
|---|---|---|---|
| Demo 導覽 - 事件旅行路線 | 這些圖表的資料從哪裡來？壞資料會被送去哪裡？ | 無 | Kafka Connect 是 Kafka 與 Elasticsearch 之間的橋樑，DLQ 是壞資料隔離路線 |
| 事件明細 - Elasticsearch 文件 | 圖表背後的 document 長什麼樣子？ | `event_id`, `event_type`, `metadata_region`, `pipeline` | SMT 後的資料形狀、`pipeline` 來源標記 |
| 事件類型趨勢 | 每分鐘不同使用者行為如何變化？ | `occurred_at`, `event_type` | Elasticsearch 聚合來自 Kafka Connect 寫入的欄位 |
| 事件總數 | Kafka Connect 是否已經把事件寫進 Elasticsearch？ | 任一 document | Sink connector 寫入結果、資料是否到達下游 |
| DLQ 壞資料數量 | 是否有 record 因解析或寫入問題被隔離？ | `raw_record`, `pipeline` | DLQ topic 也可以由另一條 sink pipeline 寫入 ES |
| DLQ 原始文件 - Elasticsearch 壞資料 | 被隔離的壞 record 寫到 Elasticsearch 後長什麼樣子？ | `raw_record`, `source_topic`, `source_partition`, `source_offset`, `dlq_timestamp`, `pipeline` | `StringConverter` 保留 raw value，`HoistField` 與 `InsertField` 補出可追查欄位 |
| 熱門商品行為統計 | 五種事件各累積多少？ | `event_type` | KQL filters、事件模型 |
| 剩餘折價券變化 | 券何時接近售罄、何時歸零？ | `remaining_coupons`, `occurred_at` | 業務觀測欄位會被原樣寫入 ES |
| 失敗原因 | 領券失敗主要因為什麼？ | `failure_reason` | event model 必須保留可分類的錯誤原因 |
| 高頻操作線索 | 是否有使用者短時間內反覆操作？ | `user_id`, `event_type` | 查詢與聚合用途會反推 event 欄位 |
| 地區流量 | 流量來自哪些地區？ | `metadata_region` | `Flatten` SMT：`metadata.region` -> `metadata_region` |

## 1. Demo 導覽 - 事件旅行路線

### 要講的知識點

這個 panel 是整場 demo 的地圖。它先建立一個最小心智模型：

```text
Application 產生 event
Kafka 接住 event
Kafka Connect 把 event 搬到 Elasticsearch
Kibana 查詢 Elasticsearch 並畫成 dashboard
無法解析或寫入的 record 會被 Kafka Connect 隔離到 DLQ
DLQ topic 會再由另一條 sink pipeline 寫入 Elasticsearch
```

### 建議講法

```text
今天不會一開始要求大家記住所有工具名稱。
先看這條資料路線：事件先進 Kafka，Kafka Connect 站在 Kafka 和 Elasticsearch 中間，負責讀 Kafka、做必要轉換、寫入 Elasticsearch。
Kibana dashboard 上的每一張圖，都是從 Elasticsearch 裡的 product-events index 查詢與聚合出來的。
如果 Kafka Connect 遇到 malformed JSON 這類壞資料，主流程仍會繼續處理其他事件；壞資料會走到 product.events.dlq，再由 DLQ sink 寫入 product-events-dlq，最後反映在 DLQ 壞資料數量 panel。
```

### 往資料源頭追

這一頁不操作資料，只用來做導航。後續每看一個 panel，都回到這張圖說明目前站在哪一段。

## 2. 事件明細 - Elasticsearch 文件

### 要講的知識點

這個 panel 展示圖表背後的一筆筆 Elasticsearch document。這些資料已經由 Kafka Connect 寫入 Elasticsearch，資料形狀會和 Kafka raw record 有些差異。

目前顯示欄位：

```text
occurred_at
event_type
user_id
coupon_id
remaining_coupons
failure_reason
metadata_region
pipeline
event_id
```

### 建議講法

```text
右邊的圖是聚合結果，左邊的表格是一筆一筆 document。
也就是說，圖表上的數字來自 Kibana 對這些 document 做時間分桶、事件類型分組與欄位聚合。
```

### 往資料源頭追

1. 在左側文件表格找一筆 `event_id`。
2. 到 Redpanda Console 的 `product.events` topic，用同一個 `event_id` 找 Kafka raw record。
3. 比較 Kafka raw record 與 ES document：

```text
Kafka raw record:
metadata.region

Elasticsearch document:
metadata_region
pipeline = connect-search-demo
```

這裡可以帶出 Kafka Connect SMT：

```text
Flatten SMT 把 metadata.region 攤平成 metadata_region。
InsertField SMT 加上 pipeline=connect-search-demo，讓我們知道資料是由這條 Kafka Connect pipeline 寫入。
```

## 3. 事件類型趨勢

### 要講的知識點

這張圖回答：

```text
活動進行時，每分鐘不同使用者行為各發生多少次？
```

Y 軸表示每分鐘的事件數。它使用：

```text
X 軸：occurred_at date histogram
Y 軸：count()
分組：event_type top values
```

### 建議講法

```text
橫軸是時間，縱軸是每分鐘事件數。
不同顏色代表不同 event_type，例如活動頁瀏覽、頁面重新整理、進入等候室、領券成功與領券失敗。
同樣是流量上升，背後可能是瀏覽增加，也可能是使用者大量重新整理或領券失敗。這張圖把不同使用者行為拆開。
```

### 往資料源頭追

1. 在圖上指一個尖峰時間點。
2. 回到事件明細 panel，觀察同一時間附近的 `event_type`。
3. 到 Redpanda Console 搜尋一筆相同 `event_id`。
4. 說明 Kafka raw record 裡已經有 `event_type`；Kafka Connect 沒有重新分類事件，只是把資料搬到 Elasticsearch。

可以帶出的 Kafka Connect 知識點：

```text
JsonConverter 將 Kafka value bytes 解析成 Kafka Connect record。
Sink connector 將 record 寫到 Elasticsearch。
Kibana 才能用 event_type 做 terms aggregation。
```

## 4. 事件總數

### 要講的知識點

這個 panel 回答：

```text
目前 Elasticsearch 裡有多少筆事件 document？
```

它是 Elasticsearch index 的 `count()`。此數字表示 Kafka Connect sink 已經寫入 Elasticsearch 的 document 數量。

### 建議講法

```text
畫面上的 24,000 代表查詢系統目前看得到 24,000 筆事件。
這個數字代表 Elasticsearch index 裡目前可查詢的 document count。
```

### 往資料源頭追

1. Redpanda Console 看 `product.events` topic 有資料。
2. Kafka Connect status 檢查 connector / task 是否 `RUNNING`。
3. 回到 dashboard 確認 Elasticsearch document count。

可以帶出的 Kafka Connect 知識點：

```text
Kafka topic 有資料，不代表 Elasticsearch 一定有資料。
中間需要 Kafka Connect sink connector 正常讀取 topic 並寫入 Elasticsearch。
```

## 5. DLQ 壞資料數量

### 要講的知識點

這個 panel 回答：

```text
資料管線有沒有遇到無法解析或無法寫入的 record？
```

正常 demo 應該是 `0`。手動送一筆 malformed JSON 後，主 Elasticsearch sink 會把壞 record 寫到 `product.events.dlq`，另一條 DLQ sink 會把這個 DLQ topic 寫入 `product-events-dlq` index，Kibana 才能看到數字。

### 建議講法

```text
這個數字代表資料管線層級的壞資料，例如 JSON 格式錯誤，Kafka Connect 無法把它解析成欄位化 record。
業務失敗請看「失敗原因」panel；資料格式或寫入問題請看 DLQ panel。
主流程不會因為這一筆壞資料停掉；壞資料會被隔離到 DLQ。
```

### 往資料源頭追

1. 手動送一筆 malformed JSON 到 `product.events`。
2. 在 Redpanda Console 看 `product.events.dlq`。
3. 在 Kibana 看 `product-events-dlq` index 的 document count。
4. 對照 connector config：

```text
主 sink：
errors.deadletterqueue.topic.name=product.events.dlq

DLQ sink：
topics=product.events.dlq
topic.to.external.resource.mapping=product.events.dlq:product-events-dlq
```

可以帶出的 Kafka Connect 知識點：

```text
DLQ 是 Kafka Connect 的錯誤隔離設計。
如果要讓 Kibana 看到 DLQ 狀態，DLQ topic 也需要被寫入 Elasticsearch。
```

## 6. DLQ 原始文件 - Elasticsearch 壞資料

### 要講的知識點

這個 panel 展示 `product-events-dlq` 裡的一筆筆 Elasticsearch document。它和「事件明細 - Elasticsearch 文件」的角色相同，但資料來源是 DLQ pipeline。

目前顯示欄位：

```text
raw_record
pipeline
source_topic
source_partition
source_offset
dlq_timestamp
```

### 建議講法

```text
DLQ 數量只能回答有幾筆壞資料。
這個表格可以看壞資料本身，也可以看它來自哪個 topic、partition 和 offset。
主 sink 解析失敗時，壞 record 被寫到 product.events.dlq；DLQ sink 再把 raw_record 和來源資訊寫入 product-events-dlq。
```

### 往資料源頭追

1. 手動送 malformed JSON 到 `product.events`。
2. 看「Kafka Connect - DLQ 壞資料數量」是否從 0 變 1。
3. 到「DLQ 原始文件 - Elasticsearch 壞資料」查看 `raw_record`。
4. 對照 Redpanda Console 的 `product.events.dlq` record。

可以帶出的 Kafka Connect 知識點：

```text
DLQ sink 使用 StringConverter 讀 raw bad record。
HoistField 把 raw value 放進 raw_record。
InsertField 補上 pipeline、source_topic、source_partition、source_offset 與 dlq_timestamp。
```

## 7. 熱門商品行為統計

### 要講的知識點

這個 panel 用 filters aggregation 依五種事件類型統計總量：

```text
COUPON_VIEWED
PAGE_REFRESHED
WAITING_ROOM_JOINED
COUPON_CLAIM_SUCCEEDED
COUPON_CLAIM_FAILED
```

總計應等於事件總數。

### 建議講法

```text
這張表把 24,000 筆事件拆成五種行為。
它統計整段活動期間各類事件的累積數量。
如果總計和事件總數一致，代表我們目前選定的五種 event_type 已經覆蓋這個 demo 的事件模型。
```

### 往資料源頭追

1. 在表格選一種事件，例如 `PAGE_REFRESHED`。
2. 到事件明細 panel 用 KQL 篩選：

```kql
event_type: PAGE_REFRESHED
```

3. 到 Redpanda Console 找同類型 raw record。

可以帶出的 Kafka Connect 知識點：

```text
Kafka Connect 不負責決定 event_type。
event_type 是 application event model 的一部分。
Kafka Connect 負責穩定搬運這些 record。
```

## 8. 剩餘折價券變化

### 要講的知識點

這張圖回答：

```text
折價券何時開始被大量領取？何時歸零？
```

它使用：

```text
X 軸：occurred_at date histogram
Y 軸：min(remaining_coupons)
```

使用 `min` 的原因是同一分鐘內可能有多筆事件。取最小值比較接近「這一分鐘結束時剩多少券」。

### 建議講法

```text
這張圖觀察 event 裡帶的業務狀態欄位 `remaining_coupons`。
當它掉到 0，代表領券服務已經回報折價券售罄。
```

### 往資料源頭追

1. 找圖上 `remaining_coupons` 下降的時間點。
2. 在事件明細 panel 看同一時間附近的 document。
3. 到 Redpanda Console 找同一筆事件，確認 Kafka raw record 內原本就帶有 `remaining_coupons`。

可以帶出的 Kafka Connect 知識點：

```text
Kafka Connect 不負責計算剩餘券數。
remaining_coupons 是業務服務產生事件時寫入的觀測欄位。
Kafka Connect 將欄位保留下來，使 Elasticsearch / Kibana 可以查詢與畫圖。
```

## 9. 失敗原因

### 要講的知識點

這個 panel 回答：

```text
領券失敗主要是售罄、限流，還是付款失敗？
```

它只統計：

```kql
event_type: COUPON_CLAIM_FAILED
```

再依 `failure_reason` 分組。

### 建議講法

```text
單看領券失敗數量還不夠。
工程與營運需要知道失敗原因是售罄、限流，還是其他系統問題。
因此 event model 必須保留 failure_reason。
```

### 往資料源頭追

1. 點出 `COUPON_SOLD_OUT` 是主要失敗原因。
2. 在事件明細 panel 篩選：

```kql
failure_reason: COUPON_SOLD_OUT
```

3. 到 Redpanda Console 找同一筆 `event_id`。

可以帶出的 Kafka Connect 知識點：

```text
錯誤原因是業務事件欄位。
Kafka Connect 的 DLQ 則是資料管線層級的壞資料隔離區，兩者不同。
```

這裡可以補一句：

```text
COUPON_CLAIM_FAILED 是業務失敗；DLQ 是 record 解析、轉換或寫入時的資料管線問題。
```

## 10. 高頻操作線索

### 要講的知識點

這個 panel 回答：

```text
是否有少數使用者短時間內反覆重新整理或多次領券失敗？
```

它使用 KQL：

```kql
event_type: PAGE_REFRESHED or event_type: COUPON_CLAIM_FAILED
```

再依 `user_id` 分組排序。它是排查線索，不代表 bot 偵測。

### 建議講法

```text
這張表用來找出需要進一步檢查的高頻操作行為。
它只提供排查線索：哪些 user_id 在這段時間內有比較多重新整理或領券失敗事件。
```

### 往資料源頭追

1. 選一個高頻 `user_id`。
2. 在事件明細 panel 篩選：

```kql
user_id: "user_01883" and (event_type: PAGE_REFRESHED or event_type: COUPON_CLAIM_FAILED)
```

3. 到 Redpanda Console 用 `user_id` 或 `event_id` 追 raw record。

可以帶出的 Kafka Connect 知識點：

```text
查詢與聚合需求會反推 event 必須有 user_id。
Kafka Connect 不知道 user 的業務意義，但它要保留欄位，讓下游可以分析。
```

## 11. 地區流量

### 要講的知識點

這個 panel 是講 SMT 的最佳入口。它依 `metadata_region` 統計地區流量。

原始 Kafka event 形狀：

```json
{
  "metadata": {
    "region": "ap-northeast-1"
  }
}
```

寫入 Elasticsearch 後的形狀：

```json
{
  "metadata_region": "ap-northeast-1",
  "pipeline": "connect-search-demo"
}
```

### 建議講法

```text
這張圖看起來只是地區統計，但它背後剛好展示 Kafka Connect 的 SMT。
application 送出的 event 裡，地區在 metadata.region 這個巢狀欄位。
Kafka Connect 在寫入 Elasticsearch 前，用 Flatten SMT 把它變成 metadata_region。
這樣 Kibana 可以直接用 metadata_region 做分組統計。
```

### 往資料源頭追

1. 在地區流量 panel 選一個 region，例如 `ap-northeast-1`。
2. 在事件明細 panel 篩選：

```kql
metadata_region: "ap-northeast-1"
```

3. 到 Redpanda Console 找同一筆 Kafka raw record。
4. 比較欄位：

```text
Kafka raw record: metadata.region
Elasticsearch document: metadata_region
```

5. 打開 connector config：

```json
"transforms": "flattenMetadata,addPipeline",
"transforms.flattenMetadata.type": "org.apache.kafka.connect.transforms.Flatten$Value",
"transforms.flattenMetadata.delimiter": "_",
"transforms.addPipeline.type": "org.apache.kafka.connect.transforms.InsertField$Value",
"transforms.addPipeline.static.field": "pipeline",
"transforms.addPipeline.static.value": "connect-search-demo"
```

可以帶出的 Kafka Connect 知識點：

```text
SMT 是 Single Message Transform。
它只處理當下這一筆 record，不做跨事件統計。
本 demo 使用 SMT 讓資料更適合 Elasticsearch / Kibana 查詢。
```

## 跨工具追資料的固定流程

每次要從圖表往資料源頭追，使用同一套流程：

1. 在 dashboard 上選定一個現象。
2. 在事件明細 panel 找一筆代表性 document。
3. 記下 `event_id`。
4. 在 Redpanda Console 的 `product.events` topic 搜尋同一個 `event_id`。
5. 比較 Kafka raw record 與 Elasticsearch document。
6. 打開 Kafka Connect connector config，指出哪個設定造成資料形狀差異。

從圖表追到資料源頭時，固定使用這個對照表：

| 觀察點 | Kafka raw record | Elasticsearch document | Kafka Connect 責任 |
|---|---|---|---|
| 原始事件類型 | `event_type` | `event_type` | 保留欄位 |
| 地區 | `metadata.region` | `metadata_region` | `Flatten` SMT |
| 來源管線 | 無 | `pipeline=connect-search-demo` | `InsertField` SMT |
| document id | Kafka record key = `event_id` | `_id = event_id` | Elasticsearch Sink 使用 key 做 upsert |
| JSON 解析 | Kafka value bytes | 欄位化 document | `JsonConverter` |

## 最後 QA 設計

建議用這些問題確認學生是否理解資料路線：

1. Dashboard 上的事件總數是 Kafka topic 裡的訊息數，還是 Elasticsearch 裡的 document 數？
2. `event_type` 是 Kafka Connect 產生的，還是 application event 產生的？
3. 為什麼 `metadata.region` 到 Elasticsearch 後變成 `metadata_region`？
4. `pipeline=connect-search-demo` 的用途是什麼？
5. `COUPON_CLAIM_FAILED` 和 DLQ 的錯誤有什麼不同？
6. 如果 Kafka topic 有資料，但 dashboard 沒資料，應該檢查哪幾個地方？

### 有獎徵答題目

正式 demo 建議只放兩題，答案必須能從 dashboard、Elasticsearch 或 connector config 直接確認。

| 題目 | 標準答案 | 驗證方式 |
|---|---|---|
| 「Kafka Connect - DLQ 壞資料數量」顯示 1，代表哪個 Elasticsearch index 裡有 1 筆 document？ | `product-events-dlq` | `curl -s http://localhost:9200/product-events-dlq/_count` |
| Kafka raw record 裡的 `metadata.region`，經過 Flatten SMT 後在 Elasticsearch document 裡變成哪個欄位？ | `metadata_region` | 對照 Redpanda Console raw record 與 Kibana 事件明細 |

標準收斂語：

```text
Kibana 讓我們看到結果。
Elasticsearch 保存可查詢的 document。
Kafka 保存事件流。
Kafka Connect 站在 Kafka 和 Elasticsearch 中間，負責解析、輕量轉換、寫入與錯誤隔離。
```
