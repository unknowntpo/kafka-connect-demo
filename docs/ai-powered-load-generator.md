# AI 驅動 Load Generator 設計

## 目標

這個 generator 的目標是產生接近真實情境的事件流，讓學生在 dashboard 上看到有意義的趨勢，而不是只有固定筆數的測試資料。

實作採用兩階段設計：

```text
LLM / AI planner
    |
    | 產生 load profile JSON
    v
Deterministic Java event generator
    |
    | 依照 profile 寫入 Kafka
    v
Kafka Connect -> Elasticsearch -> Kibana
    |
    | score-load-profile.sh
    v
Feedback loop and profile tuning
```

LLM 不應逐筆產生每一個 event。逐筆產生會太慢、成本高、難以重現，也不容易測試。較合理的做法是讓 AI 產生 scenario characteristics：phases、event weights、inventory、failure modes、participant users 與 time shape。Java generator 再依照這些特徵穩定產生可重跑的事件。

## 範例情境：限量折價券搶購

使用者故事：

```text
一批限量折價券開放領取。
每個參與者至少先瀏覽活動頁。
大量使用者重新整理頁面，部分使用者進入 waiting room。
一部分使用者成功領券。
折價券數量歸零後，失敗事件開始上升。
```

profile 檔案：

```text
profiles/flash-sale-coupon.json
```

重要欄位：

- `total_events`：目標事件數。
- `participant_users`：參與活動的使用者數；每個 participant 會先產生一筆 `COUPON_VIEWED`。
- `duration_seconds`：要產生的歷史時間窗。
- `inventory`：限量折價券數量。
- `time_skew_power`：控制事件是否集中在時間窗後段。
- `phases`：行為階段，例如 teaser、waiting room、drop open、sold-out pressure。
- `event_weights`：活動頁瀏覽之後，各階段後續行為的事件類型權重。
- `sold_out_event_weights`：inventory 歸零後的事件類型權重。
- `failure_weights`：各階段的 failure reasons 權重。

## AI 的價值

AI 適合用來產生 load profile，而不是取代 event generator。

好的 AI-generated profile 可以描述：

- business context：coupon drop、product launch、checkout outage、influencer campaign。
- behavioral phases：warmup、queue buildup、release、sold-out pressure、long tail。
- event mix：refreshes、views、queue joins、claim attempts、successes、failures。
- constraint causality：有限 inventory 會導致後段出現 `COUPON_SOLD_OUT`。
- user realism：大量使用者參與，少數使用者較活躍，但不應由單一使用者主導整個 stream。
- data quality cases：仍可注入 malformed records，用來驗證 DLQ。

## 回饋迴圈

feedback loop 透過 Elasticsearch queries 實作：

```bash
./scripts/score-load-profile.sh
```

scorer 會檢查：

- 總事件量是否足以支撐 dashboard 趨勢。
- 後 30 分鐘流量是否明顯高於前 30 分鐘。
- refresh 與 waiting-room events 是否存在。
- coupon claims 是否在 inventory 歸零前成功。
- inventory 歸零後，sold-out failures 是否成為主要失敗原因。
- user distribution 是否足夠分散，且每個活躍使用者都有對應的 `COUPON_VIEWED` 入口事件。
- Kafka Connect SMT 產生的欄位是否存在：`pipeline=connect-search-demo` 用於 provenance，`metadata_region` 用於 dashboard grouping。

產生並評分目前的 profile：

```bash
./scripts/seed-ai-load-profile.sh
```

這個腳本預設會先清理 demo state、重建 Elasticsearch mapping、註冊 connector、建立 Kibana dashboard、透過 Kafka 產生 profile events、等待 indexing 完成，最後輸出 score。

cleanup 會移除：

- sink connector
- Kafka data topics
- Kafka Connect internal topics
- Elasticsearch index

內建 flash-sale profile 預設讓事件結束於執行當下：

```text
BASE_TIME=<current UTC time>
```

因此重跑時資料會完整落在目前時間之前，打開 dashboard 的 `now-3h` 到 `now` 時間窗就能看到整段事件。事件內容、比例與波形仍由固定 seed 與 profile 控制；若需要完全固定時間，可以手動指定 `BASE_TIME` 或 `EVENT_START_TIME`。

## 擴充方式

真正接上 LLM 時，可以讓 LLM 產生相同 JSON contract 的檔案：

```text
profiles/<scenario>.json
```

prompt 方向範例：

```text
Generate a Kafka event load profile for a limited coupon flash sale.
Return JSON only. Include participant_users, phases, event weights,
failure weights, inventory, duration, total event count,
and expected dashboard signals.
```

只要 JSON contract 不變，deterministic generator 與 scorer 不需要修改。這讓 AI profile 可以持續演進，同時保留可測試、可重跑的 demo 行為。
