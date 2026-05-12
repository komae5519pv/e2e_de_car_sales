# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 07 | Genie スペース作成手順
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">テーブルに日本語で話しかける — 自然言語 BI を体験する</p>
# MAGIC     </div>
# MAGIC     <div style="margin-left: auto;">
# MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 20 min</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #FFC107; background: #FFF8E1; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🎯 このノートブックのゴール</strong><br>
# MAGIC ここまでに作った中古車販売の Silver / Gold / Metric Views テーブルを使って、<b>Genie スペース</b>を作成します。<br>
# MAGIC Genie に日本語で話しかけて「SQL を書かずにデータ分析」する体験をしましょう！
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li>Genie スペースの作成手順を確認する</li>
# MAGIC   <li>General Instructions（Genie への業務ルール指示）を中古車業界向けに設定する</li>
# MAGIC   <li>サンプル質問（通常モード × 3 + エージェントモード × 3）で「Wow!」を体験する</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📋 前提条件</strong><br>
# MAGIC ✅ <code>03_SDPパイプライン設定手順.py</code> 実行済み(<code>sl_*</code> / <code>gd_*</code> 作成済み)<br>
# MAGIC ✅ <code>04_NB補完処理.py</code> 実行済み(<code>sl_inquiries_enriched</code> 等の AI enrich テーブル作成済み)<br>
# MAGIC ✅ <code>05_テーブル設定.py</code> 実行済み(コメント・PK/FK 設定済み — Genie の精度向上に必須)<br>
# MAGIC ✅ <code>06_UC_Metrics_Views.py</code> 実行済み(<code>mv_sales_kpi</code> / <code>mv_inventory_kpi</code> / <code>mv_inquiry_kpi</code> 作成済み)<br>
# MAGIC ✅ サーバーレス SQL ウェアハウスが起動中
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,共通設定の読み込み
# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,Genie スペース作成時に UI に入力する値(00_config から取得)
displayHTML(f"""
<div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
<strong>🎯 Genie スペースに登録するカタログ・スキーマ</strong><br>
<table style="margin-top: 8px;">
  <tr><td><b>カタログ</b></td><td><code>{MY_CATALOG}</code></td></tr>
  <tr><td><b>スキーマ</b></td><td><code>{MY_SCHEMA}</code></td></tr>
</table>
</div>
""")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 1. Genie とは
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">1. Genie とは</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 Genie Space とは</strong><br>
# MAGIC テーブルを登録するだけで、<b>自然言語(日本語 OK)</b>でデータに問い合わせできる AI 機能です。<br>
# MAGIC SQL を書けない業務担当者(店長・エリアマネージャー・営業)でも<br>
# MAGIC 「先月最も粗利率が高かった店舗は？」と聞くだけでデータを取得できます。<br><br>
# MAGIC <strong>Genie が賢く動くためのポイント:</strong>
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li>テーブル / カラムに <b>コメント</b> を付ける(05 で設定済み)</li>
# MAGIC   <li><b>PK / FK 制約</b> を宣言する(05 で設定済み — テーブル間のリレーションを Genie が理解)</li>
# MAGIC   <li><b>UC Metric Views</b> をソースに使う(06 で作成済み — 業務指標が定義済みなので Genie が指標を間違えない)</li>
# MAGIC   <li><b>General Instructions</b> で Genie の振る舞いを指示する</li>
# MAGIC   <li><b>サンプル質問</b> を登録する(回答精度が大きく向上)</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 2. Genie スペースの作成手順
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">2. Genie スペースの作成手順</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: Genie スペースを作成する</strong><br><br>
# MAGIC <strong>手順:</strong>
# MAGIC <ol style="margin: 6px 0 0 0;">
# MAGIC   <li>左メニューの「<b>Genie</b>」をクリック</li>
# MAGIC   <li>右上「<b>New</b>」 → 「<b>Genie space</b>」をクリック</li>
# MAGIC   <li>タイトルを入力: <b>中古車販売 E2E 分析</b></li>
# MAGIC   <li>テーブルを追加(下の 10 テーブル)</li>
# MAGIC   <li>SQL ウェアハウスを選択(サーバーレス推奨)</li>
# MAGIC   <li>「<b>Save</b>」をクリック</li>
# MAGIC </ol>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Genie スペースに登録するテーブル一覧
displayHTML(f"""
<div style="border-left: 4px solid #7B1FA2; background: #F3E5F5; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
<strong>🔧 設定内容</strong>
<table style="width: 100%; border-collapse: collapse; border-radius: 8px; overflow: hidden; margin-top: 8px;">
  <thead>
    <tr style="background: #7B1FA2; color: white;">
      <th style="padding: 10px 15px; text-align: left;">設定項目</th>
      <th style="padding: 10px 15px; text-align: left;">値</th>
    </tr>
  </thead>
  <tbody>
    <tr style="background: #f9f9f9;">
      <td style="padding: 8px 15px;"><b>タイトル</b></td>
      <td style="padding: 8px 15px;">中古車販売 E2E 分析</td>
    </tr>
    <tr>
      <td style="padding: 8px 15px;"><b>説明</b></td>
      <td style="padding: 8px 15px;">中古車販売の店舗・在庫・顧客・商談・成約・相場・AI 分類済み引合データを自然言語で分析できます</td>
    </tr>
    <tr style="background: #f9f9f9; vertical-align: top;">
      <td style="padding: 8px 15px;"><b>テーブル(10)</b></td>
      <td style="padding: 8px 15px;">
        <b>Metric Views(集計指標 ★Genie の主役):</b><br>
        <code>{MY_CATALOG}.{MY_SCHEMA}.mv_sales_kpi</code> — 成約 KPI<br>
        <code>{MY_CATALOG}.{MY_SCHEMA}.mv_inventory_kpi</code> — 在庫 KPI<br>
        <code>{MY_CATALOG}.{MY_SCHEMA}.mv_inquiry_kpi</code> — 引合 KPI<br><br>
        <b>Silver(明細):</b><br>
        <code>{MY_CATALOG}.{MY_SCHEMA}.sl_stores</code> — 店舗マスタ<br>
        <code>{MY_CATALOG}.{MY_SCHEMA}.sl_vehicles</code> — 在庫車両マスタ<br>
        <code>{MY_CATALOG}.{MY_SCHEMA}.sl_customers</code> — 顧客マスタ<br>
        <code>{MY_CATALOG}.{MY_SCHEMA}.sl_contracts</code> — 成約明細<br>
        <code>{MY_CATALOG}.{MY_SCHEMA}.sl_market_index</code> — 中古車相場指数<br><br>
        <b>AI enrich / Gold:</b><br>
        <code>{MY_CATALOG}.{MY_SCHEMA}.sl_inquiries_enriched</code> — AI で分類済みの引合<br>
        <code>{MY_CATALOG}.{MY_SCHEMA}.gd_customer_rm_segment</code> — 顧客 RM セグメント
      </td>
    </tr>
  </tbody>
</table>
</div>
""")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 Metric Views を主役にする理由</strong><br>
# MAGIC <code>mv_sales_kpi</code> 等の Metric Views は <b>業務指標(契約件数・売上合計・粗利率・在庫回転日数 等)が定義済み</b>です。<br>
# MAGIC これにより、Genie は「粗利率」を独自解釈せずに <b>常に同じ計算式</b>で答えるため、回答のブレが大幅に減ります。<br>
# MAGIC Silver テーブルも併せて登録することで、Metric Views に無い切り口の質問(例: 「修復歴あり vs なし」)にも対応できます。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 3. General Instructions の設定
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">3. General Instructions の設定</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 General Instructions とは</strong><br>
# MAGIC Genie への「業務ルール」や「回答スタイル」の指示です。<br>
# MAGIC ここに書いた内容を踏まえて、Genie が SQL を生成します。<br>
# MAGIC 例: 「金額は万円で表示して」と書くと、Genie は <code>ROUND(amount / 10000, 1)</code> のような SQL を生成します。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ General Instructions — 以下をそのままコピーしてください</strong><br>
# MAGIC Genie スペースの設定画面 → 「<b>Instructions</b>」タブに貼り付けます。
# MAGIC </div>
# MAGIC
# MAGIC ```
# MAGIC あなたは中古車販売チェーンのデータ分析アシスタントです。
# MAGIC 店長・エリアマネージャー・営業担当からの質問に、店舗・在庫・顧客・商談・成約・相場データを使って回答します。
# MAGIC
# MAGIC ■ ドメイン用語(中古車業界):
# MAGIC - 年式: 車両の登録年(例: 2020年式)。新しいほど高価
# MAGIC - 走行距離: km 単位。少ないほど高価
# MAGIC - 修復歴: 事故等による骨格部位の修復経験。あり/なしで価格・査定が大きく変わる
# MAGIC - 認定中古車: メーカー / 販売店が品質保証した中古車。プレミア価格
# MAGIC - ボディタイプ: 軽自動車 / コンパクト / セダン / SUV / ミニバン / 輸入車
# MAGIC - 粗利率: (販売価格 - 仕入価格) / 販売価格。中古車店の収益性指標
# MAGIC - 在庫回転日数: 仕入れてから売れるまでの日数。短いほど良い
# MAGIC - 商談チャネル: 来店 / オンライン / 電話 / 買取査定
# MAGIC - 引合カテゴリ: 04 で AI 分類済み(購入検討 / 値引交渉 / 買取査定 / 在庫確認 / その他)
# MAGIC
# MAGIC ■ 対応できる分析:
# MAGIC - 成約分析: 店舗別・月別・ボディタイプ別の契約件数・売上・粗利
# MAGIC - 在庫分析: 在庫台数・平均販売予定価格・在庫構成・在庫回転
# MAGIC - 引合分析: 引合件数・成約率・チャネル別動向・AI 分類カテゴリ別動向
# MAGIC - 顧客分析: RM セグメント(VIP / Loyal / At Risk / Lost / Prospect)別の特徴
# MAGIC - 相場影響: 中古車相場指数の変動と販売動向の関係
# MAGIC - 修復歴・認定中古車の影響: 価格・粗利率・在庫回転への影響
# MAGIC
# MAGIC ■ 回答のルール:
# MAGIC - 金額は日本円で、万円単位(例: 1,234万円)で表示してください
# MAGIC - 走行距離は km 単位、年式は西暦(例: 2020年)で表示してください
# MAGIC - 粗利率はパーセント(例: 12.3%)で表示してください
# MAGIC - 集計指標(契約件数・売上合計・粗利率 等)を聞かれたら、まず Metric Views(mv_sales_kpi / mv_inventory_kpi / mv_inquiry_kpi) を優先的に使ってください
# MAGIC - 明細レベルの質問(個別車両・個別顧客)は Silver テーブル(sl_*)を使ってください
# MAGIC - 表やグラフで視覚的にわかりやすく回答してください
# MAGIC - 分析結果には、現場で使える示唆を一言添えてください
# MAGIC ```

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 4. サンプル質問を登録する
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">4. サンプル質問を登録する</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 サンプル質問を登録する理由</strong><br>
# MAGIC サンプル質問は「こういう聞き方にはこう答えて」というお手本です。<br>
# MAGIC 登録しておくと、似た質問が来たときに Genie の回答精度がぐっと上がります。<br>
# MAGIC Genie スペースの設定画面 → 「<b>Sample questions</b>」タブに登録します。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 通常モード vs エージェントモード</strong><br>
# MAGIC Genie には 2 つのモードがあります:
# MAGIC <table style="margin-top: 8px; border-collapse: collapse; width: 100%;">
# MAGIC   <tr style="background: #1B3139; color: white;">
# MAGIC     <th style="padding: 8px; text-align: left;">モード</th>
# MAGIC     <th style="padding: 8px; text-align: left;">得意なこと</th>
# MAGIC     <th style="padding: 8px; text-align: left;">動き方</th>
# MAGIC   </tr>
# MAGIC   <tr style="background: #f9f9f9;">
# MAGIC     <td style="padding: 8px;"><b>通常モード</b></td>
# MAGIC     <td style="padding: 8px;">1 つのテーブルに対するシンプルな質問</td>
# MAGIC     <td style="padding: 8px;">SQL を 1 本生成して即回答</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 8px;"><b>エージェントモード</b></td>
# MAGIC     <td style="padding: 8px;">複数テーブルを跨ぐ複雑な分析</td>
# MAGIC     <td style="padding: 8px;">質問を分解 → 複数 SQL を順番に実行 → 結果を統合して回答</td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ サンプル質問(通常モード) — 以下を登録してください</strong>
# MAGIC </div>
# MAGIC
# MAGIC | # | サンプル質問 | 対象テーブル |
# MAGIC |---|---|---|
# MAGIC | 1 | 先月、契約件数が多かった店舗トップ 5 を教えて | mv_sales_kpi |
# MAGIC | 2 | 都道府県別の在庫台数と平均販売予定価格を比較して | mv_inventory_kpi |
# MAGIC | 3 | 来店チャネル別の引合件数と成約率を見せて | mv_inquiry_kpi |

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #7B1FA2; background: #F3E5F5; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ サンプル質問(エージェントモード) — 以下も登録してください</strong><br>
# MAGIC 複数テーブルを組み合わせる質問です。エージェントが自動的に分析計画を立てて実行します。
# MAGIC </div>
# MAGIC
# MAGIC | # | サンプル質問 | 組み合わせるテーブル |
# MAGIC |---|---|---|
# MAGIC | 4 | 修復歴ありの車両の販売実績を、修復歴なしと粗利率・在庫回転で比較して | sl_vehicles + sl_contracts |
# MAGIC | 5 | VIP 顧客が直近 6 ヶ月で買っているボディタイプの傾向を分析して、品揃え戦略を提案して | gd_customer_rm_segment + sl_contracts + sl_vehicles |
# MAGIC | 6 | 中古車相場指数が下落した月で、成約率や粗利率がどう変動したか分析して | sl_market_index + mv_sales_kpi + mv_inquiry_kpi |

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 5. Genie に話しかけてみよう！
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">5. Genie に話しかけてみよう！</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 まずは通常モードで体験</strong><br><br>
# MAGIC Genie スペースを開いて、以下の質問を試してみてください。<br>
# MAGIC 自然言語が SQL に変換される様子に注目！<br><br>
# MAGIC
# MAGIC <blockquote style="border-left: 3px solid #388E3C; padding-left: 12px; color: #2E7D32; font-style: italic; margin: 8px 0;">
# MAGIC 「先月の店舗別の売上合計を教えて」
# MAGIC </blockquote>
# MAGIC
# MAGIC <blockquote style="border-left: 3px solid #388E3C; padding-left: 12px; color: #2E7D32; font-style: italic; margin: 8px 0;">
# MAGIC 「ボディタイプ別の在庫台数と平均販売予定価格を教えて」
# MAGIC </blockquote>
# MAGIC
# MAGIC <blockquote style="border-left: 3px solid #388E3C; padding-left: 12px; color: #2E7D32; font-style: italic; margin: 8px 0;">
# MAGIC 「年式別(2018 / 2019 / 2020 / 2021 年)の平均販売価格を比較して」
# MAGIC </blockquote>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #7B1FA2; background: #F3E5F5; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 次はエージェントモードを体験！</strong><br><br>
# MAGIC 入力欄の左にある <b>エージェントモードのトグル</b> を ON にして、以下を試してください。<br>
# MAGIC Genie が質問を分解 → 複数の SQL を順番に実行 → 結果を統合して回答する様子が見られます！<br><br>
# MAGIC
# MAGIC <blockquote style="border-left: 3px solid #7B1FA2; padding-left: 12px; color: #6A1B9A; font-style: italic; margin: 8px 0;">
# MAGIC 「修復歴あり vs なしで、粗利率と在庫回転日数にどれくらい差があるか分析して」
# MAGIC </blockquote>
# MAGIC
# MAGIC <blockquote style="border-left: 3px solid #7B1FA2; padding-left: 12px; color: #6A1B9A; font-style: italic; margin: 8px 0;">
# MAGIC 「VIP 顧客と Loyal 顧客で、買っている車のボディタイプや平均価格はどう違う？」
# MAGIC </blockquote>
# MAGIC
# MAGIC <blockquote style="border-left: 3px solid #7B1FA2; padding-left: 12px; color: #6A1B9A; font-style: italic; margin: 8px 0;">
# MAGIC 「相場指数が下がった月に、当社の成約率と粗利率はどう動いた？相場と当社業績の連動を分析して」
# MAGIC </blockquote>
# MAGIC
# MAGIC <blockquote style="border-left: 3px solid #7B1FA2; padding-left: 12px; color: #6A1B9A; font-style: italic; margin: 8px 0;">
# MAGIC 「AI 分類された引合カテゴリ(購入検討 / 値引交渉 / 買取査定)別に成約率を比較して、どのカテゴリの引合に注力すべきか提案して」
# MAGIC </blockquote>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 うまくいかないときは</strong><br>
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li>質問を <b>より具体的</b> にしてみてください(「売上」 → 「月別の売上合計」)</li>
# MAGIC   <li>Genie が生成した <b>SQL を確認</b> して、意図通りか見てみましょう</li>
# MAGIC   <li>フィードバック(👍 👎)を送ると、Genie の精度が向上します</li>
# MAGIC   <li>サンプル質問を追加すると、似た質問の精度が上がります</li>
# MAGIC   <li>集計値が想定と合わないときは <b>Metric Views(mv_*)</b> を使うようにヒントしてみてください</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ Genie スペース作成完了</strong><br>
# MAGIC SQL を 1 行も書かずに、自然言語だけでデータ分析ができることを体験しました。<br>
# MAGIC <b>テーブルコメント・PK/FK 制約・Metric Views</b> をしっかり整備しておくことで、Genie の精度が大きく向上します。<br><br>
# MAGIC 次は <code>08_ダッシュボード作成手順.py</code> で AI/BI ダッシュボードを作り、ビジュアル分析を体験しましょう。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ トラブルシューティング</strong><br>
# MAGIC <b>Genie が「テーブルが見つかりません」と返す</b>: テーブル名が完全修飾(<code>catalog.schema.table</code>)で登録されているか確認<br>
# MAGIC <b>集計値がブレる</b>: Metric Views(<code>mv_*</code>) を優先使用するよう General Instructions に明記済みか確認<br>
# MAGIC <b>「修復歴」「認定中古車」を理解しない</b>: General Instructions のドメイン用語セクションを再確認 / カラムコメントを確認(05 で設定済み)<br>
# MAGIC <b>SQL ウェアハウスがタイムアウト</b>: サーバーレス SQL ウェアハウスのサイズを Small → Medium に変更<br>
# MAGIC <b>エージェントモードが遅い</b>: 複数 SQL を順次実行するため、通常モードより 30 秒〜 1 分長くなることがあります(正常)<br>
# MAGIC <b>日本語の質問で精度が低い</b>: テーブル / カラムコメントが日本語で書かれているか確認(05 で日本語コメント設定済み)
# MAGIC </div>
