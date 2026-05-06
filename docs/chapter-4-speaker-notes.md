# Kafka Connect Demo 講者備忘

這份文件是講者備忘，不放進 blog 或 dashboard 正文。

## Demo 模式

- Demo 是 deterministic profile replay；`run-demo` 會使用 realtime pacing，讓 30 秒事件窗逐步寫入 Kafka。
- 現場展示建議使用 `just run-demo`，避免逐步指令漏掉清理步驟。
- 如果聽眾問「為什麼不是即時流」，可以說明：Kafka Connect 持續消費 Kafka topic；本 demo 為了課堂可重複性，使用固定 seed 重播同一段流量，事件開始時間則對齊執行當下。

## 用語界線

- 不要說「Elasticsearch 不能承受 indexing」。較精確的說法是：application 層不應同步承擔搜尋與觀測寫入責任。
- 不要說「Kafka Connect 保證 exactly-once」。本 demo 以 at-least-once 理解，並用穩定 `event_id` 作為 Elasticsearch document id 降低重送影響。
- 不要把「高頻操作線索」直接稱為 bot 偵測。現有資料產生器沒有建立明確 bot cohort，因此只能說它提供排查線索。
- `熱門商品行為統計` 是五種 event type 的 filter count；加總應等於事件總數，但不是完整 funnel 或 conversion rate。

## 現場檢查順序

1. 確認 Google Slides：`Kafka Connect Ch4`。
2. 確認 Docker 空間：`docker system df`。
3. 重播 demo：`just run-demo`。
4. 若 Kibana saved object 回 429，檢查 Elasticsearch flood-stage watermark，必要時清理 Docker disk 並解除 read-only block。
5. 若需要完整驗證，執行 `just e2e`。

## 有獎徵答

這題安排在 `just run-demo` 跑完、手動送 malformed JSON 進 DLQ 之後。題目要有可驗證的唯一答案，避免變成開放式討論。

### 題目：壞資料會被放到哪裡？

題目：

```text
Kafka Connect 讀到 malformed JSON 這類壞資料時，會把它放到哪個錯誤隔離區？
```

標準答案：

```text
DLQ / Dead Letter Queue
```

可現場驗證：

```text
Kibana panel: Kafka Connect - DLQ 壞資料數量
Kafka topic: product.events.dlq
```
