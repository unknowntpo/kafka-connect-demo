---
theme: default
title: Kafka Connect Live Demo - 熱門商品銷售觀測
info: |
  這份 Slidev 投影片只做 live demo 開場。先用比喻建立 Kafka、Kafka Connect、Elasticsearch、Kibana 的共同語言，再切到現場 dashboard。
class: text-left
drawings:
  persist: false
transition: slide-left
mdc: true
---

<section class="kc-slide">
  <h1 class="kc-title">先認識四個角色</h1>
  <p class="kc-subtitle">Think of the demo as a busy coupon campaign site</p>

  <div class="kc-concepts">
    <div class="kc-concept">
      <div class="kc-icon">01</div>
      <div class="kc-tag">Kafka</div>
      <div class="kc-use-title">
        <strong>活動事件的等候區</strong>
        <em>Event waiting area</em>
      </div>
      <p>頁面瀏覽、重新整理、進等候室、領券結果，都先排進 `product.events`。</p>
      <div class="kc-example">像櫃檯前的號碼牌：先接住每件事。</div>
    </div>

    <div class="kc-concept">
      <div class="kc-icon">02</div>
      <div class="kc-tag">Connect</div>
      <div class="kc-use-title">
        <strong>搬運工</strong>
        <em>Data mover</em>
      </div>
      <p>Kafka Connect 從 Kafka 搬資料到 Elasticsearch，搬運時整理欄位、貼上 pipeline 標籤。</p>
      <div class="kc-example">壞資料另外送到 DLQ，不堵住主路線。</div>
    </div>

    <div class="kc-concept">
      <div class="kc-icon">03</div>
      <div class="kc-tag">ES</div>
      <div class="kc-use-title">
        <strong>可搜尋的事件倉庫</strong>
        <em>Searchable document store</em>
      </div>
      <p>Elasticsearch 把事件變成文件，讓我們快速查詢、過濾、聚合。</p>
      <div class="kc-example">像倉庫索引：知道每筆資料在哪裡。</div>
    </div>

    <div class="kc-concept">
      <div class="kc-icon">04</div>
      <div class="kc-tag">Kibana</div>
      <div class="kc-use-title">
        <strong>活動觀察大螢幕</strong>
        <em>Operations dashboard</em>
      </div>
      <p>Kibana 讀 Elasticsearch，把事件畫成趨勢、表格、地區流量與 DLQ 數字。</p>
      <div class="kc-example">等一下 live demo 會從這裡開始。</div>
    </div>
  </div>
</section>

---

<section class="kc-slide">
  <h1 class="kc-title">實際用在哪裡?</h1>
  <p class="kc-subtitle">Common Use Cases in This Kafka Connect Demo</p>

  <p class="kc-lead">用限量折價券搶領事件，先看見 dashboard，再回頭拆資料路線。</p>

  <div class="kc-usecases">
    <div class="kc-usecase">
      <div class="kc-icon">01</div>
      <div class="kc-tag">SEARCH</div>
      <div class="kc-use-title">
        <strong>事件寫入 Elasticsearch</strong>
        <em>Indexing Kafka Events for Search</em>
      </div>
      <p>Kafka Connect 讀取 `product.events`，把事件穩定寫到 `product-events`。</p>
      <div class="kc-example">例: 從 dashboard 點回一筆 Elasticsearch document。</div>
    </div>

    <div class="kc-usecase">
      <div class="kc-icon">02</div>
      <div class="kc-tag">OBSERVE</div>
      <div class="kc-use-title">
        <strong>用 Dashboard 看活動壓力</strong>
        <em>Observing Traffic and Failures</em>
      </div>
      <p>Kibana 呈現事件趨勢、總數、失敗原因、剩餘折價券與地區流量。</p>
      <div class="kc-example">例: 發現 17:00 附近流量暴衝與領券失敗。</div>
    </div>

    <div class="kc-usecase">
      <div class="kc-icon">03</div>
      <div class="kc-tag">DLQ</div>
      <div class="kc-use-title">
        <strong>隔離壞資料</strong>
        <em>Dead Letter Queue for Bad Records</em>
      </div>
      <p>Malformed JSON 進 `product.events.dlq`，再寫到 `product-events-dlq`。</p>
      <div class="kc-example">例: 手動送壞 JSON 後，DLQ count 從 0 變 1。</div>
    </div>
  </div>
</section>

---

<section class="kc-slide kc-shot-slide">
  <h1 class="kc-title">熱門商品銷售觀測</h1>
  <p class="kc-subtitle">The dashboard is the entry point, not the appendix.</p>

  <div class="kc-band">Live demo starts here</div>

  <div class="kc-shot-wrap">
    <img src="/live-demo-dashboard.png" class="kc-shot" />
  </div>

  <p class="kc-footnote">下一步切到 Kibana: 從圖表、文件、DLQ count 追到 Kafka Connect 設定。</p>
</section>

<style>
.slidev-layout {
  background: #f5f8fa;
  color: #21295c;
}

.kc-slide {
  position: relative;
  width: 100%;
  height: 100%;
  padding: 26px 48px 24px;
  background: #f5f8fa;
}

.kc-title {
  margin: 0;
  color: #21295c;
  font-family: Georgia, "Times New Roman", serif;
  font-size: 38px;
  font-weight: 700;
  letter-spacing: 0;
}

.kc-subtitle {
  margin: 4px 0 22px;
  color: #00627d;
  font-family: Georgia, "Times New Roman", serif;
  font-size: 15px;
  font-style: italic;
}

.kc-lead {
  margin: 0 0 18px;
  color: #4e5f73;
  font-size: 18px;
}

.kc-band {
  height: 40px;
  display: flex;
  align-items: center;
  padding: 0 18px;
  background: #00627d;
  color: #fff;
  font-size: 18px;
  font-weight: 700;
}

.kc-usecases {
  margin-top: 18px;
  border-top: 1px solid #d4dde6;
  border-bottom: 1px solid #d4dde6;
}

.kc-concepts,
.kc-usecases {
  margin-top: 18px;
  border-top: 1px solid #d4dde6;
  border-bottom: 1px solid #d4dde6;
}

.kc-concept,
.kc-usecase {
  display: grid;
  grid-template-columns: 72px 84px 250px 1fr 240px;
  gap: 12px;
  align-items: center;
  min-height: 94px;
  border-bottom: 1px solid #d4dde6;
  background: #fff;
  box-shadow: inset 10px 0 0 #00627d;
  padding: 8px 10px 8px 26px;
}

.kc-concept {
  min-height: 82px;
}

.kc-concept:last-child,
.kc-usecase:last-child {
  border-bottom: 0;
}

.kc-icon {
  width: 52px;
  height: 52px;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 999px;
  background: #00627d;
  color: #fff;
  font-family: Georgia, "Times New Roman", serif;
  font-size: 20px;
  font-weight: 700;
}

.kc-tag {
  border: 1.5px solid #247da0;
  color: #00627d;
  background: #eef6fa;
  text-align: center;
  padding: 10px 6px;
  font-family: Georgia, "Times New Roman", serif;
  font-size: 13px;
  font-weight: 700;
}

.kc-use-title strong {
  display: block;
  color: #21295c;
  font-size: 18px;
  font-weight: 700;
}

.kc-use-title em {
  display: block;
  margin-top: 8px;
  color: #5a6878;
  font-family: Georgia, "Times New Roman", serif;
  font-size: 12px;
  font-style: italic;
}

.kc-concept p,
.kc-usecase p {
  margin: 0;
  color: #39485a;
  font-size: 14px;
  line-height: 1.55;
}

.kc-example {
  border: 1.5px solid #f0a52c;
  background: #fff9ef;
  color: #5a4a2f;
  padding: 11px 12px;
  font-size: 13px;
  line-height: 1.45;
}

.kc-shot-slide {
  padding-bottom: 18px;
}

.kc-shot-wrap {
  margin-top: 10px;
  padding: 10px;
  border: 1px solid #d4dde6;
  background: #fff;
}

.kc-shot {
  display: block;
  width: 100%;
  height: 330px;
  object-fit: cover;
  object-position: top left;
}

.kc-footnote {
  margin: 9px 0 0;
  color: #5a6878;
  text-align: center;
  font-family: Georgia, "Times New Roman", serif;
  font-size: 14px;
  font-style: italic;
}
</style>
