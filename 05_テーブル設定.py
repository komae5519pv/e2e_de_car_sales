# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 05 | テーブル設定(コメント・PK/FK・PII マスキング)
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">辞書ベースで全テーブルにコメント・PK/FK・PII マスキングを一括適用</p>
# MAGIC     </div>
# MAGIC     <div style="margin-left: auto;">
# MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 10 min</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #FFC107; background: #FFF8E1; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🎯 このノートブックのゴール</strong><br>
# MAGIC 02-04 で作成した <b>全テーブル</b>に対して、Catalog Explorer / Lineage / Genie が活用するメタデータを一括適用します。<br>
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li><b>テーブル / カラムコメント</b> - 業務ドメイン用語で説明(Genie が文脈理解に使う)</li>
# MAGIC   <li><b>主キー / 外部キー</b> - SDP テーブル(<code>sl_*</code>)は 02 で <b>CREATE 句にインライン宣言済み</b>。本 NB では 04 で作った非 SDP テーブルのみ補強</li>
# MAGIC   <li><b>PII マスキング関数</b> - 顧客名 / 電話 / VIN / ナンバープレート に列レベルマスクを適用</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📋 前提条件</strong><br>
# MAGIC ✅ <code>03_SDPパイプライン設定手順.py</code> 実行済み(<code>bz_*</code> / <code>sl_*</code> / <code>gd_*</code> = 15 テーブル)<br>
# MAGIC ✅ <code>04_NB補完処理.py</code> 実行済み(enrich 系 5 テーブル)<br>
# MAGIC ✅ Unity Catalog 環境(PK/FK・列マスク はすべて UC 機能)
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 PK/FK は Informational</strong><br>
# MAGIC Databricks の <code>PRIMARY KEY</code> / <code>FOREIGN KEY</code> は <b>強制されない情報制約</b>です(<code>NOT ENFORCED</code>)。<br>
# MAGIC それでも設定する理由:<br>
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li><b>Catalog Explorer</b>: テーブル間の関係が UI で可視化される</li>
# MAGIC   <li><b>Genie</b>: 自然言語クエリで JOIN を自動推論できる</li>
# MAGIC   <li><b>クエリオプティマイザ</b>: PK ユニーク性を前提にした最適化が効く</li>
# MAGIC </ul>
# MAGIC <br>
# MAGIC <strong>📝 SDP テーブル(<code>sl_*</code>)の DDL ルール</strong><br>
# MAGIC SDP の Streaming Table / Materialized View は <code>ALTER TABLE ... ADD CONSTRAINT</code> が <b>不可</b>です。<br>
# MAGIC PK/FK は <b>02_SDPパイプライン定義.sql</b> の <code>CREATE OR REFRESH STREAMING TABLE</code> 句に <b>インライン宣言</b>済み。<br>
# MAGIC コメント・列マスクの DDL は <code>ALTER STREAMING TABLE</code> を使います(本 NB で使用)。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,共通設定の読み込み
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 1. テーブルコメントを一括適用
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 1. テーブルコメントを一括適用</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC テーブル名→コメントの辞書を 1 つ用意して <code>ALTER TABLE ... SET TBLPROPERTIES</code> ではなく
# MAGIC <code>COMMENT ON TABLE</code> で一括適用します。<br>
# MAGIC ➡ <b>SDP の <code>COMMENT</code> 句</b> でも同じことができますが、ここでは <b>04 で notebook 作成したテーブル</b>も含めて一元管理。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,テーブルコメント辞書(Bronze は簡潔、Silver/Gold は詳細多行)
TABLE_COMMENTS = {
    # ===== Bronze (取り込み生データ。詳細は Silver で説明) =====
    "bz_stores":                  "店舗マスタ Bronze。Volume CSV を Auto Loader で取り込んだ生データ。",
    "bz_vehicles":                "車両在庫 Bronze。Volume CSV を Auto Loader で取り込んだ生データ。",
    "bz_customers":               "顧客マスタ Bronze。Volume CSV を Auto Loader で取り込んだ生データ。",
    "bz_market_index":            "中古車相場指数 Bronze。Volume CSV を Auto Loader で取り込んだ生データ。",
    "bz_inquiries":               "商談履歴 Bronze。履歴バルク + 直近 5 日分の日次 CSV を増分取り込み。",
    "bz_contracts":               "成約・粗利 Bronze。履歴バルク + 直近 5 日分の日次 CSV を増分取り込み。",

    # ===== Silver (SDP Streaming Table) =====
    "sl_stores": """テーブル名:`sl_stores / 店舗マスタ Silver`
説明:中古車販売店舗の基本情報を管理します。中古車プラザ・買取専門・輸入車専門の3業態 30店舗を、緯度経度・開店日・所在地など店舗運営に必要な属性とともに格納します。商圏分析・店舗別販売実績・地理可視化の起点となるテーブルです。""",

    "sl_vehicles": """テーブル名:`sl_vehicles / 車両在庫マスタ Silver`
説明:販売店に在庫している中古車1台ごとの仕様・状態・価格を管理します。年式・走行距離・修復歴・認定中古車フラグ・VIN17桁など、商談・査定・成約の判断材料となる項目を網羅。Bronze から取り込み後、Expectations で価格整合性と VIN 桁数を検証済み。
補足:
* 下記カラムを参照する外部キー
`sl_stores.store_id`""",

    "sl_customers": """テーブル名:`sl_customers / 顧客マスタ Silver`
説明:来店・問合せ・成約に至った顧客の基本属性とライフスタイル情報を管理します。氏名・電話などの PII 属性は列マスクで保護。年収レンジ・家族構成・ライフステージ・希望ボディタイプ・予算上限の5項目は約45%の顧客で NULL となっており、04 NB 補完処理で `ai_query` による推論補完を行います。
補足:
* 下記カラムを参照する外部キー
`sl_stores.store_id`""",

    "sl_inquiries": """テーブル名:`sl_inquiries / 商談履歴 Silver`
説明:顧客が来店・オンライン・電話・買取査定で接触した1件ごとの商談を記録します。inquiry_memo(自由記述メモ)は 04 NB 補完処理で `ai_classify`(カテゴリ分類)と `ai_extract`(希望条件抽出)の入力ソースとして利用。Bronze から増分で Auto Loader 取り込み。
補足:
* 下記カラムを参照する外部キー
`sl_customers.customer_id`
`sl_stores.store_id`
`sl_vehicles.vehicle_id`""",

    "sl_contracts": """テーブル名:`sl_contracts / 成約・粗利 Silver`
説明:商談が成約した1件ごとの契約と粗利を管理します。販売価格・仕入価格・粗利・粗利率を保持し、Expectations で `actual_price - purchase_price = gross_profit` の整合性を検証済み。月次の店舗別売上・粗利分析、04 の NTILE 粗利スコアリング、RM セグメンテーションの基礎データ。
補足:
* 下記カラムを参照する外部キー
`sl_inquiries.inquiry_id`
`sl_customers.customer_id`
`sl_stores.store_id`
`sl_vehicles.vehicle_id`""",

    "sl_market_index": """テーブル名:`sl_market_index / 中古車相場指数 Silver`
説明:日次の中古車相場・燃料価格・USD/JPY 為替を車種カテゴリ別に管理します。相場指数は基準=100.0 で正規化、過去2年分の時系列データ。Gold の相場連動分析(`gd_market_linked_margin`)の入力。輸入車需要は為替に強く相関するためダッシュボードで合わせて可視化します。""",

    # ===== Gold (SDP Materialized View) =====
    "gd_store_monthly_sales": """テーブル名:`gd_store_monthly_sales / 店舗×月別 販売実績マート Gold`
説明:`sl_contracts` と `sl_stores` を結合し、店舗 × 月単位で販売台数・売上・粗利を集計したマートです。AI/BI Dashboard と Genie Space の主軸テーブル。前月比・前年同月比・店舗ランキングなど、経営層・店長向け KPI の分母分子に使われます。Materialized View として増分リフレッシュ。
補足:
* 下記カラムを参照する外部キー
`sl_stores.store_id`""",

    "gd_vehicle_inventory": """テーブル名:`gd_vehicle_inventory / ボディタイプ別 在庫サマリ Gold`
説明:`sl_vehicles` を集計し、ボディタイプ × 在庫ステータス(在庫中・商談中・成約済・出庫済)別の台数と平均価格を可視化するマートです。在庫回転状況・売れ筋ボディタイプの把握に利用。Materialized View として増分リフレッシュ。""",

    "gd_market_linked_margin": """テーブル名:`gd_market_linked_margin / 相場連動 粗利分析 Gold`
説明:`sl_contracts` × `sl_market_index` を月次×車種カテゴリで結合し、相場指数の変動に対する販売粗利率の感応度を分析するマートです。相場高騰時に値引きが進んでいないか・相場下落時に粗利が削られていないかを月次でモニタリング。輸入車は為替(`usd_jpy_rate`)も並列指標として利用。""",

    # ===== Silver (04 NB 補完処理 / 通常 Delta) =====
    "sl_inquiries_enriched": """テーブル名:`sl_inquiries_enriched / 商談履歴 + AI 分類・希望条件抽出`
説明:`sl_inquiries.inquiry_memo` に対して `ai_classify` でカテゴリラベル(購入検討/値引交渉/買取査定/在庫確認/その他)を付与し、`ai_extract` で希望ボディタイプ・予算上限・希望メーカーの3フィールドを STRUCT で抽出した拡張テーブルです。デモ用に LIMIT 100 で生成。
補足:
* 下記カラムを参照する外部キー
`sl_customers.customer_id`
`sl_stores.store_id`
`sl_vehicles.vehicle_id`""",

    "sl_customers_enriched": """テーブル名:`sl_customers_enriched / 顧客属性 AI 推論補完`
説明:`sl_customers` で NULL となっている年収レンジ・家族構成・ライフステージ・希望ボディタイプ・予算上限の5項目を、その顧客の `inquiry_memo`(商談メモ)を全件結合し `ai_query` で推論補完した拡張テーブルです。`databricks-meta-llama-3-3-70b-instruct` で JSON レスポンスを取得し `from_json` で STRUCT 化。デモ用に NULL 属性顧客のうち30名のみ。
補足:
* 下記カラムを参照する外部キー
`sl_customers.customer_id`""",

    "sl_pdf_catalogs_parsed": """テーブル名:`sl_pdf_catalogs_parsed / 車両カタログ PDF パース結果`
説明:Volume `/Volumes/.../pdf_catalogs/` に配置された5車種(ハリアー・ヴェゼル・シエンタ・ノート・N-BOX)の PDF カタログを `ai_parse_document` で構造化テキスト化したテーブルです。後続の 07 RAG 構築(Vector Search)の入力ソースとして利用します。""",

    # ===== Gold (04 NB 補完処理 / 通常 Delta) =====
    "gd_contract_margin_score": """テーブル名:`gd_contract_margin_score / 契約 粗利スコア Gold`
説明:`sl_contracts` に対して、店舗別に粗利率(`gross_margin`)を `NTILE(5)` でスコア化(1=最低〜5=最高)したテーブルです。「店舗内で粗利が高い案件・低い案件はどれか」をダッシュボードで可視化する用途。SDP の宣言的な MV では書きづらいウィンドウ関数のため 04 NB 補完処理で生成。
補足:
* 下記カラムを参照する外部キー
`sl_contracts.contract_id`
`sl_customers.customer_id`
`sl_stores.store_id`
`sl_vehicles.vehicle_id`""",

    "gd_customer_rm_segment": """テーブル名:`gd_customer_rm_segment / 顧客 RM セグメント Gold`
説明:中古車は購買頻度が極小(生涯2-5回)のため、RFM ではなく RM(Recency × Monetary)で顧客を VIP / Loyal / At Risk / Lost / Prospect の5区分にセグメンテーションしたテーブルです。最終接点経過月数(`recency_months`)と累計取引額のパーセンタイルランクで判定。CRM・リテンション施策の起点。
補足:
* 下記カラムを参照する外部キー
`sl_customers.customer_id`""",
}

print(f"対象テーブル数: {len(TABLE_COMMENTS)}")

# COMMAND ----------

# DBTITLE 1,テーブルコメントを一括適用
for table, comment in TABLE_COMMENTS.items():
    # COMMENT ON TABLE はシングルクォートのエスケープに注意
    safe_comment = comment.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {table} IS '{safe_comment}'")
print(f"✅ {len(TABLE_COMMENTS)} 個のテーブルコメントを適用しました")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 2. カラムコメントを一括適用(Silver / Gold 全 14 テーブル)
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 2. カラムコメントを一括適用(Silver / Gold 全 14 テーブル)</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <b>Genie が業務用語を理解する</b>ためにカラムコメントは特に重要です。<br>
# MAGIC ここでは Silver 6 + Gold 3(SDP) + Silver 3 + Gold 2(04 NB 補完処理)= <b>合計 14 テーブル</b>のカラムに、<br>
# MAGIC <b>業務名称・型・例 / 値リスト / 主キー or 外部キー参照</b> をビジネス文脈とともに付与します。<br>
# MAGIC <b>適用構文はオブジェクト種別で自動振り分け</b>:
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li>SDP Streaming Table(<code>sl_*</code> 6 個) → <code>ALTER STREAMING TABLE ... ALTER COLUMN ... COMMENT '...'</code></li>
# MAGIC   <li>SDP Materialized View(<code>gd_store_monthly_sales</code> / <code>gd_vehicle_inventory</code> / <code>gd_market_linked_margin</code>) → <code>ALTER MATERIALIZED VIEW ... ALTER COLUMN ... COMMENT '...'</code></li>
# MAGIC   <li>通常 Delta(04 で生成した enrich 系 5 テーブル) → <code>ALTER TABLE ... ALTER COLUMN ... COMMENT '...'</code></li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,カラムコメント辞書 {table: {column: comment}}
COLUMN_COMMENTS = {
    # ===== Silver (SDP Streaming Table) =====
    "sl_stores": {
        "store_id":      "店舗ID、文字列、ユニーク(主キー)、形式 'STORE_NNNN'",
        "store_name":    "店舗名、文字列、例 '中古車プラザ横浜店'",
        "format":        "店舗業態、文字列、例 '中古車プラザ', '買取専門', '輸入車専門'",
        "prefecture":    "所在都道府県、文字列、例 '東京都', '神奈川県'",
        "city":          "所在市区町村、文字列、例 '横浜市西区'",
        "latitude":      "緯度、浮動小数点、地図系可視化に使用、例 35.4658",
        "longitude":     "経度、浮動小数点、地図系可視化に使用、例 139.6222",
        "opened_date":   "開店日、YYYY-MM-DDフォーマット",
    },
    "sl_vehicles": {
        "vehicle_id":      "車両ID、文字列、ユニーク(主キー)、形式 'VEH_NNNNNN'",
        "maker":           "メーカー名、文字列、例 'トヨタ', 'ホンダ', '日産', 'BMW'",
        "model":           "車種名、文字列、例 'ハリアー', 'ヴェゼル', 'シエンタ'",
        "body_type":       "ボディタイプ、文字列、例 '軽自動車', 'コンパクト', 'セダン', 'SUV', 'ミニバン', '輸入車'",
        "model_year":      "年式(西暦)、整数、例 2020",
        "mileage_km":      "走行距離(km)、整数",
        "repair_history":  "修復歴、文字列、例 'あり', 'なし'",
        "certified":       "認定中古車フラグ、ブール、例 true=認定中古車",
        "vin":             "車台番号(17桁)、文字列、PII マスキング対象(`mask_vin` 関数)",
        "plate_number":    "ナンバープレート、文字列、PII マスキング対象(`mask_plate` 関数)",
        "grade":           "グレード、文字列、例 'G', 'X', 'Z'",
        "purchase_price":  "仕入価格、整数(円)",
        "listing_price":   "販売価格(店頭表示)、整数(円)",
        "stock_in_date":   "入庫日、YYYY-MM-DDフォーマット",
        "status":          "在庫ステータス、文字列、例 '在庫中', '商談中', '成約済', '出庫済'",
        "store_id":        "所属店舗ID、文字列、`sl_stores.store_id`を参照する外部キー",
    },
    "sl_customers": {
        "customer_id":           "顧客ID、文字列、ユニーク(主キー)、形式 'CUST_NNNNNN'",
        "name":                  "氏名、文字列、PII マスキング対象(`mask_name` 関数)",
        "age":                   "年齢、整数",
        "gender":                "性別、文字列、例 'M', 'F'",
        "phone":                 "電話番号、文字列、PII マスキング対象(`mask_phone` 関数)",
        "area":                  "居住エリア(都道府県名)、文字列、例 '東京都'",
        "registered_store_id":   "登録店舗ID、文字列、`sl_stores.store_id`を参照する外部キー",
        "first_contact_date":    "初回接点日、YYYY-MM-DDフォーマット",
        "annual_income_range":   "年収レンジ、文字列、例 '〜400万', '400-600万', '600-800万', '800-1000万', '1000万〜'、約45%が NULL(04 で AI 補完)",
        "family_structure":      "家族構成、文字列、例 '独身', '夫婦のみ', '子育て世代', '子育て後', 'シニア'、約45%が NULL(04 で AI 補完)",
        "lifestage":             "ライフステージ、文字列、例 '新社会人', '結婚適齢期', '子育て世代', '子育て後', 'シニア'、約45%が NULL(04 で AI 補完)",
        "preferred_body_type":   "希望ボディタイプ、文字列、約45%が NULL(04 で AI 補完)",
        "budget_max":            "予算上限、整数(円)、約45%が NULL(04 で AI 補完)",
    },
    "sl_inquiries": {
        "inquiry_id":     "商談ID、文字列、ユニーク(主キー)、形式 'INQ_NNNNNNN'",
        "customer_id":    "顧客ID、文字列、`sl_customers.customer_id`を参照する外部キー",
        "store_id":       "店舗ID、文字列、`sl_stores.store_id`を参照する外部キー",
        "vehicle_id":     "対象車両ID、文字列、`sl_vehicles.vehicle_id`を参照する外部キー",
        "inquiry_date":   "商談発生日、YYYY-MM-DDフォーマット",
        "channel":        "接触チャネル、文字列、例 '来店', 'オンライン', '電話', '買取査定'",
        "status":         "商談ステータス、文字列、例 '商談中', '成約', '失注', '保留'",
        "inquiry_memo":   "商談メモ(自由記述)、文字列、04 NB で `ai_classify` / `ai_extract` の入力ソース",
    },
    "sl_contracts": {
        "contract_id":     "契約ID、文字列、ユニーク(主キー)、形式 'CTR_NNNNNN'",
        "inquiry_id":      "元商談ID、文字列、`sl_inquiries.inquiry_id`を参照する外部キー",
        "customer_id":     "顧客ID、文字列、`sl_customers.customer_id`を参照する外部キー",
        "store_id":        "店舗ID、文字列、`sl_stores.store_id`を参照する外部キー",
        "vehicle_id":      "成約車両ID、文字列、`sl_vehicles.vehicle_id`を参照する外部キー",
        "contract_date":   "契約日、YYYY-MM-DDフォーマット",
        "listing_price":   "店頭表示価格、整数(円)",
        "actual_price":    "実成約価格(値引後)、整数(円)",
        "purchase_price":  "仕入価格、整数(円)",
        "gross_profit":    "粗利、整数(円)、計算式 actual_price - purchase_price",
        "gross_margin":    "粗利率、浮動小数点、計算式 gross_profit / actual_price",
        "payment_method":  "支払方法、文字列、例 '現金', 'ローン', '残価設定'",
    },
    "sl_market_index": {
        "date":              "観測日、YYYY-MM-DDフォーマット、`vehicle_category`との複合主キー",
        "vehicle_category":  "車種カテゴリ、文字列、`sl_vehicles.body_type`と同分類、`date`との複合主キー",
        "market_index":      "中古車相場指数、浮動小数点、基準=100.0 で正規化",
        "regular_gas_price": "レギュラーガソリン価格、浮動小数点(円/L)",
        "usd_jpy_rate":      "USD/JPY 為替レート、浮動小数点、輸入車需要に強く相関",
    },

    # ===== Gold (SDP Materialized View) =====
    "gd_store_monthly_sales": {
        "store_id":           "店舗ID、文字列、`sl_stores.store_id`を参照する外部キー",
        "store_name":         "店舗名、文字列、`sl_stores`から JOIN で取得",
        "prefecture":         "店舗所在都道府県、文字列、`sl_stores`から JOIN で取得",
        "format":             "店舗業態、文字列、`sl_stores`から JOIN で取得",
        "sales_month":        "売上集計月、YYYY-MM-DDフォーマット(月初日)",
        "contracts_count":    "成約件数、整数、月内の契約レコード数",
        "sales_amount":       "売上総額、整数(円)、`SUM(actual_price)`",
        "gross_profit_total": "粗利総額、整数(円)、`SUM(gross_profit)`",
        "avg_gross_margin":   "平均粗利率、浮動小数点、`AVG(gross_margin)`",
    },
    "gd_vehicle_inventory": {
        "body_type":             "ボディタイプ、文字列、例 '軽自動車', 'SUV' 等",
        "status":                "在庫ステータス、文字列、例 '在庫中', '商談中', '成約済', '出庫済'",
        "vehicles_count":        "車両台数、整数、(`body_type`, `status`)別の台数",
        "avg_listing_price":     "平均販売価格、浮動小数点(円)、`AVG(listing_price)`",
        "avg_mileage_km":        "平均走行距離、浮動小数点(km)、`AVG(mileage_km)`",
        "repair_history_count":  "修復歴あり台数、整数、`SUM(CASE WHEN repair_history='あり' THEN 1)`",
    },
    "gd_market_linked_margin": {
        "month":              "集計月、YYYY-MM-DDフォーマット(月初日)",
        "vehicle_category":   "車種カテゴリ、文字列、`sl_vehicles.body_type`と同分類",
        "avg_market_index":   "月平均相場指数、浮動小数点、`sl_market_index`から `AVG(market_index)`",
        "avg_usd_jpy":        "月平均 USD/JPY、浮動小数点、輸入車カテゴリで参照",
        "contracts_count":    "成約件数、整数、(`month`, `vehicle_category`)別",
        "avg_gross_margin":   "平均粗利率、浮動小数点、`AVG(gross_margin)`",
    },

    # ===== Silver (04 NB 補完処理 / 通常 Delta) =====
    "sl_inquiries_enriched": {
        "inquiry_id":            "商談ID、文字列、ユニーク(主キー)、`sl_inquiries.inquiry_id`と一致",
        "customer_id":           "顧客ID、文字列、`sl_customers.customer_id`を参照する外部キー",
        "store_id":              "店舗ID、文字列、`sl_stores.store_id`を参照する外部キー",
        "vehicle_id":            "対象車両ID、文字列、`sl_vehicles.vehicle_id`を参照する外部キー",
        "inquiry_date":          "商談発生日、YYYY-MM-DDフォーマット",
        "channel":               "接触チャネル、文字列、例 '来店', 'オンライン', '電話', '買取査定'",
        "status":                "商談ステータス、文字列、例 '商談中', '成約', '失注'",
        "inquiry_memo":          "商談メモ(自由記述)、文字列、AI 分類・抽出の元ソース",
        "inquiry_category":      "AI 分類された商談カテゴリ、文字列、例 '購入検討', '値引交渉', '買取査定', '在庫確認', 'その他'(`ai_classify` 結果)",
        "extracted_conditions":  "AI 抽出した希望条件、STRUCT<preferred_body_type:STRING, budget_max_yen:STRING, preferred_maker:STRING>(`ai_extract` 結果)",
    },
    "sl_customers_enriched": {
        "customer_id":   "顧客ID、文字列、ユニーク(主キー)、`sl_customers.customer_id`を参照する外部キー",
        "memo_concat":   "商談メモを `concat_ws(' / ', collect_list(inquiry_memo))` で連結した文字列、AI 推論の入力",
        "ai_json":       "`ai_query` の生レスポンス JSON 文字列、デバッグ・監査用",
        "inferred":      "AI 推論結果、STRUCT<annual_income_range:STRING, family_structure:STRING, lifestage:STRING, preferred_body_type:STRING, budget_max:BIGINT>(`from_json` 後)",
    },
    "sl_pdf_catalogs_parsed": {
        "path":          "PDF ファイルの Volume パス、文字列、例 '/Volumes/.../pdf_catalogs/harrier.pdf'",
        "catalog_name":  "カタログ識別名、文字列、ファイル名から拡張子を除いた値、例 'harrier', 'vezel'",
        "parsed":        "`ai_parse_document` の構造化結果、STRUCT、後続 RAG(Vector Search)の入力",
        "_ingested_at":  "取り込みタイムスタンプ、TIMESTAMP、`current_timestamp()`",
    },

    # ===== Gold (04 NB 補完処理 / 通常 Delta) =====
    "gd_contract_margin_score": {
        "contract_id":     "契約ID、文字列、ユニーク(主キー)、`sl_contracts.contract_id`を参照する外部キー",
        "store_id":        "店舗ID、文字列、`sl_stores.store_id`を参照する外部キー、NTILE のパーティションキー",
        "vehicle_id":      "成約車両ID、文字列、`sl_vehicles.vehicle_id`を参照する外部キー",
        "customer_id":     "顧客ID、文字列、`sl_customers.customer_id`を参照する外部キー",
        "contract_date":   "契約日、YYYY-MM-DDフォーマット",
        "actual_price":    "実成約価格(値引後)、整数(円)",
        "gross_profit":    "粗利、整数(円)",
        "gross_margin":    "粗利率、浮動小数点、NTILE の ORDER BY キー",
        "margin_score":    "粗利率スコア、整数、`NTILE(5) OVER (PARTITION BY store_id ORDER BY gross_margin)` の結果、例 1=最低, 5=最高",
    },
    "gd_customer_rm_segment": {
        "customer_id":         "顧客ID、文字列、ユニーク(主キー)、`sl_customers.customer_id`を参照する外部キー",
        "last_contract_date":  "最終契約日、YYYY-MM-DDフォーマット、`MAX(contract_date)`、契約なしの場合 NULL",
        "total_spend":         "累計取引額、整数(円)、`SUM(actual_price)`、契約なしは 0",
        "recency_months":      "最終接点経過月数、浮動小数点、`months_between(current_date(), last_contract_date)`",
        "rm_segment":          "RM セグメント、文字列、例 'VIP'(直近12ヶ月以内 & 取引額上位30%), 'Loyal'(直近12ヶ月以内 & 中位/下位), 'At Risk'(12ヶ月超 & 取引額上位30%), 'Lost'(12ヶ月超 & 中位/下位), 'Prospect'(契約履歴なし)",
    },
}

# COMMAND ----------

# DBTITLE 1,カラムコメントを一括適用(オブジェクト種別で ALTER 構文を自動振り分け)
# SDP Streaming Table → ALTER STREAMING TABLE
SDP_STREAMING_TABLES = {
    "sl_stores", "sl_vehicles", "sl_customers",
    "sl_inquiries", "sl_contracts", "sl_market_index",
}
# SDP Materialized View → ALTER MATERIALIZED VIEW
SDP_MATERIALIZED_VIEWS = {
    "gd_store_monthly_sales", "gd_vehicle_inventory", "gd_market_linked_margin",
}
# それ以外(04 で生成した通常 Delta)→ ALTER TABLE

total_cols = 0
for table, cols in COLUMN_COMMENTS.items():
    if table in SDP_STREAMING_TABLES:
        prefix = f"ALTER STREAMING TABLE {table}"
    elif table in SDP_MATERIALIZED_VIEWS:
        prefix = f"ALTER MATERIALIZED VIEW {table}"
    else:
        prefix = f"ALTER TABLE {table}"
    for col, comment in cols.items():
        safe_comment = comment.replace("'", "''")
        spark.sql(f"{prefix} ALTER COLUMN {col} COMMENT '{safe_comment}'")
        total_cols += 1
print(f"✅ {len(COLUMN_COMMENTS)} 個のテーブル / 計 {total_cols} カラムにコメントを適用しました")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 3. 主キー(PK) - 02 でインライン宣言済み
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 3. 主キー(PK) - 02 でインライン宣言済み</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 SDP では ALTER による PK 追加は不可</strong><br>
# MAGIC SDP Streaming Table は <code>ALTER TABLE ... ADD CONSTRAINT PRIMARY KEY</code> をサポートしません。<br>
# MAGIC PK は <b>02_SDPパイプライン定義.sql</b> の <code>CREATE OR REFRESH STREAMING TABLE</code> 句で <b>インライン宣言</b>しています:<br>
# MAGIC <pre style="background: #FFFFFF; padding: 8px; font-size: 11px;">CREATE OR REFRESH STREAMING TABLE sl_stores (
# MAGIC #   store_id STRING NOT NULL,
# MAGIC #   ...
# MAGIC #   CONSTRAINT pk_sl_stores PRIMARY KEY (store_id)
# MAGIC # ) ...</pre>
# MAGIC <b>Silver PK</b>:
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li><code>sl_stores.store_id</code></li>
# MAGIC   <li><code>sl_vehicles.vehicle_id</code></li>
# MAGIC   <li><code>sl_customers.customer_id</code></li>
# MAGIC   <li><code>sl_inquiries.inquiry_id</code></li>
# MAGIC   <li><code>sl_contracts.contract_id</code></li>
# MAGIC   <li><code>sl_market_index.(date, vehicle_category)</code> -- 複合キー</li>
# MAGIC </ul>
# MAGIC <b>Gold MV PK</b>(<code>CREATE OR REFRESH MATERIALIZED VIEW</code> にインライン宣言):
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li><code>gd_store_monthly_sales.(store_id, sales_month)</code> -- 複合キー</li>
# MAGIC   <li><code>gd_vehicle_inventory.(body_type, status)</code> -- 複合キー</li>
# MAGIC   <li><code>gd_market_linked_margin.(month, vehicle_category)</code> -- 複合キー</li>
# MAGIC </ul>
# MAGIC <b>04 Delta テーブル PK</b> は次のセクション(Step 4-2)で <code>ALTER TABLE ADD CONSTRAINT</code> で追加します。<br>
# MAGIC ➡ Step 7 の <code>information_schema.table_constraints</code> で適用結果を確認できます。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 4. 外部キー(FK) - 02 でインライン宣言済み
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 4. 外部キー(FK) - 02 でインライン宣言済み</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC FK も PK と同様、SDP では <code>ALTER ... ADD CONSTRAINT FOREIGN KEY</code> 不可のため、<br>
# MAGIC <b>02_SDPパイプライン定義.sql</b> の <code>CREATE OR REFRESH STREAMING TABLE / MATERIALIZED VIEW</code> 句にインライン宣言済み。<br>
# MAGIC <b>Silver FK (9 本)</b>:
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li><code>sl_vehicles.store_id</code> → <code>sl_stores.store_id</code></li>
# MAGIC   <li><code>sl_customers.registered_store_id</code> → <code>sl_stores.store_id</code></li>
# MAGIC   <li><code>sl_inquiries.customer_id</code> → <code>sl_customers.customer_id</code></li>
# MAGIC   <li><code>sl_inquiries.store_id</code> → <code>sl_stores.store_id</code></li>
# MAGIC   <li><code>sl_inquiries.vehicle_id</code> → <code>sl_vehicles.vehicle_id</code></li>
# MAGIC   <li><code>sl_contracts.inquiry_id</code> → <code>sl_inquiries.inquiry_id</code></li>
# MAGIC   <li><code>sl_contracts.customer_id</code> → <code>sl_customers.customer_id</code></li>
# MAGIC   <li><code>sl_contracts.store_id</code> → <code>sl_stores.store_id</code></li>
# MAGIC   <li><code>sl_contracts.vehicle_id</code> → <code>sl_vehicles.vehicle_id</code></li>
# MAGIC </ul>
# MAGIC <b>Gold MV FK (1 本)</b>:
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li><code>gd_store_monthly_sales.store_id</code> → <code>sl_stores.store_id</code></li>
# MAGIC </ul>
# MAGIC <b>04 Delta テーブル FK</b> は次のセクション(Step 4-2)で <code>ALTER TABLE ADD CONSTRAINT</code> で追加します。<br>
# MAGIC ➡ Genie が自然言語で「店舗別の販売台数」と聞かれたとき、FK 経由で <code>JOIN</code> を自動推論します。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 4-2. 04 Delta テーブルに PK/FK を追加(ALTER TABLE)
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 4-2. 04 Delta テーブルに PK/FK を追加(ALTER TABLE)</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC 04 で作成した <b>非 SDP の Delta テーブル</b>(<code>sl_inquiries_enriched</code> / <code>sl_customers_enriched</code> / <code>sl_pdf_catalogs_parsed</code> / <code>gd_contract_margin_score</code> / <code>gd_customer_rm_segment</code>)に対して、<br>
# MAGIC <code>ALTER TABLE ADD CONSTRAINT</code> で PK / FK を追加します。<br>
# MAGIC <b>Delta PK 設定の 3 ステップ</b>(idempotent な再実行可能パターン):
# MAGIC <ol style="margin: 6px 0 0 0;">
# MAGIC   <li><code>ALTER TABLE ... ALTER COLUMN ... SET NOT NULL</code> -- PK 列は NOT NULL 必須</li>
# MAGIC   <li><code>ALTER TABLE ... DROP CONSTRAINT IF EXISTS</code> -- 再実行時の冪等性</li>
# MAGIC   <li><code>ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY</code></li>
# MAGIC </ol>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,04 Delta テーブルの PK 定義
# (テーブル名, [PK カラム]) のリスト
DELTA_PRIMARY_KEYS = [
    ("sl_inquiries_enriched",   ["inquiry_id"]),
    ("sl_customers_enriched",   ["customer_id"]),
    ("sl_pdf_catalogs_parsed",  ["catalog_name"]),
    ("gd_contract_margin_score",["contract_id"]),
    ("gd_customer_rm_segment",  ["customer_id"]),
]

for table_name, pk_cols in DELTA_PRIMARY_KEYS:
    fq_table = f"{MY_CATALOG}.{MY_SCHEMA}.{table_name}"
    pk_constraint = f"pk_{table_name}"
    pk_cols_csv   = ", ".join(pk_cols)

    # 1) NOT NULL を保証
    for col in pk_cols:
        spark.sql(f"ALTER TABLE {fq_table} ALTER COLUMN {col} SET NOT NULL")

    # 2) 既存 PK を冪等に削除
    spark.sql(f"ALTER TABLE {fq_table} DROP CONSTRAINT IF EXISTS {pk_constraint}")

    # 3) PK を追加
    spark.sql(f"ALTER TABLE {fq_table} ADD CONSTRAINT {pk_constraint} PRIMARY KEY ({pk_cols_csv})")

    print(f"✅ PK 設定: {table_name} ({pk_cols_csv})")

# COMMAND ----------

# DBTITLE 1,04 Delta テーブルの FK 定義
# (子テーブル, FK 列, 親テーブル, 親 PK 列) のリスト
DELTA_FOREIGN_KEYS = [
    # sl_inquiries_enriched (3 FKs)
    ("sl_inquiries_enriched", "customer_id", "sl_customers", "customer_id"),
    ("sl_inquiries_enriched", "store_id",    "sl_stores",    "store_id"),
    ("sl_inquiries_enriched", "vehicle_id",  "sl_vehicles",  "vehicle_id"),

    # sl_customers_enriched (1 FK)
    ("sl_customers_enriched", "customer_id", "sl_customers", "customer_id"),

    # gd_contract_margin_score (4 FKs)
    ("gd_contract_margin_score", "contract_id", "sl_contracts", "contract_id"),
    ("gd_contract_margin_score", "store_id",    "sl_stores",    "store_id"),
    ("gd_contract_margin_score", "vehicle_id",  "sl_vehicles",  "vehicle_id"),
    ("gd_contract_margin_score", "customer_id", "sl_customers", "customer_id"),

    # gd_customer_rm_segment (1 FK)
    ("gd_customer_rm_segment", "customer_id", "sl_customers", "customer_id"),
]

for child_table, fk_col, parent_table, parent_pk in DELTA_FOREIGN_KEYS:
    fq_child  = f"{MY_CATALOG}.{MY_SCHEMA}.{child_table}"
    fq_parent = f"{MY_CATALOG}.{MY_SCHEMA}.{parent_table}"
    fk_constraint = f"fk_{child_table}_{fk_col}"

    # 既存 FK を冪等に削除 → 追加
    spark.sql(f"ALTER TABLE {fq_child} DROP CONSTRAINT IF EXISTS {fk_constraint}")
    spark.sql(
        f"ALTER TABLE {fq_child} "
        f"ADD CONSTRAINT {fk_constraint} "
        f"FOREIGN KEY ({fk_col}) REFERENCES {fq_parent}({parent_pk})"
    )

    print(f"✅ FK 設定: {child_table}.{fk_col} → {parent_table}.{parent_pk}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ 04 Delta テーブルの PK 5 本 / FK 9 本 を追加</strong><br>
# MAGIC これで Silver SDP(02 インライン)+ Gold MV(02 インライン)+ 04 Delta(本セクション)の <b>全テーブルに PK/FK</b> が設定されました。<br>
# MAGIC Catalog Explorer の Relationships タブで ER 図が確認できます。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 5. PII マスキング関数を定義
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 5. PII マスキング関数を定義</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC UC の <b>列マスク(Column Mask)</b> 機能を使って、特権ユーザー以外には PII を伏字にします。<br>
# MAGIC <code>is_account_group_member('admin_users')</code> でグループ判定 → 該当しなければマスク。<br>
# MAGIC <b>マスク対象</b>: 顧客名 / 電話番号 / VIN / ナンバープレート の 4 列。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ デモ環境での注意</strong><br>
# MAGIC <code>admin_users</code> グループが workspace に存在しない場合、関数は常にマスクを適用します(= 自分でも伏字で見える)。<br>
# MAGIC これはデモ目的では問題ありません。本番では既存のグループ名(例: <code>account users</code>)に書き換えてください。
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 顧客名: 姓のみ残す(山田 太郎 → 山田 ***)
# MAGIC CREATE OR REPLACE FUNCTION mask_name(name STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('admin_users') THEN name
# MAGIC   ELSE concat(split(name, ' ')[0], ' ***')
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 電話番号: 先頭 3 桁のみ表示(090-1234-5678 → 090-****-****)
# MAGIC CREATE OR REPLACE FUNCTION mask_phone(phone STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('admin_users') THEN phone
# MAGIC   ELSE concat(substring(phone, 1, 3), '-****-****')
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VIN(車台番号 17 桁): 先頭 4 桁のみ表示(WBA12345... → WBA1*************)
# MAGIC CREATE OR REPLACE FUNCTION mask_vin(vin STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('admin_users') THEN vin
# MAGIC   ELSE concat(substring(vin, 1, 4), repeat('*', greatest(length(vin) - 4, 0)))
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ナンバープレート: 全文マスク(ひらがな 1 文字 + ****)
# MAGIC CREATE OR REPLACE FUNCTION mask_plate(plate STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('admin_users') THEN plate
# MAGIC   ELSE '*** **-**'
# MAGIC END;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 6. 列マスクを各テーブルに適用
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 6. 列マスクを各テーブルに適用</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC SDP テーブルなので <code>ALTER STREAMING TABLE ... ALTER COLUMN ... SET MASK &lt;function&gt;</code> で列に関数を貼り付けます。<br>
# MAGIC 既存マスクを <code>DROP MASK</code> で剥がしてから貼ることで、再実行時にエラーにならないように。<br>
# MAGIC ➡ 初回実行時は既存マスクが無いので <code>DROP MASK</code> がエラーになります。<code>try/except</code> で吸収します。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,列マスク定義 [(table, column, mask_function), ...]
COLUMN_MASKS = [
    ("sl_customers", "name",         "mask_name"),
    ("sl_customers", "phone",        "mask_phone"),
    ("sl_vehicles",  "vin",          "mask_vin"),
    ("sl_vehicles",  "plate_number", "mask_plate"),
]

for table, col, fn in COLUMN_MASKS:
    # 初回実行で既存マスクが無い場合は DROP MASK が失敗するので吸収
    try:
        spark.sql(f"ALTER STREAMING TABLE {table} ALTER COLUMN {col} DROP MASK")
    except Exception:
        pass
    spark.sql(f"ALTER STREAMING TABLE {table} ALTER COLUMN {col} SET MASK {fn}")
    print(f"  ✓ {table}.{col:15s} → {fn}()")

print(f"\n✅ {len(COLUMN_MASKS)} 個の列にマスクを適用しました")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 7. 適用結果を確認
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 7. 適用結果を確認</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- マスキングが効いているか確認(admin_users グループに入っていなければ伏字で出る)
# MAGIC SELECT customer_id, name, phone
# MAGIC FROM sl_customers
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VIN とナンバープレートのマスキングを確認
# MAGIC SELECT vehicle_id, maker, model, vin, plate_number
# MAGIC FROM sl_vehicles
# MAGIC LIMIT 5;

# COMMAND ----------

# DBTITLE 1,PK / FK の一覧を information_schema から確認
display(spark.sql(f"""
SELECT
  table_name,
  constraint_name,
  constraint_type
FROM system.information_schema.table_constraints
WHERE table_catalog = '{MY_CATALOG}'
  AND table_schema  = '{MY_SCHEMA}'
ORDER BY table_name, constraint_type, constraint_name
"""))

# COMMAND ----------

# DBTITLE 1,テーブルコメントが付いているか確認
display(spark.sql(f"""
SELECT table_name, comment
FROM system.information_schema.tables
WHERE table_catalog = '{MY_CATALOG}'
  AND table_schema  = '{MY_SCHEMA}'
  AND comment IS NOT NULL
ORDER BY table_name
"""))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ テーブル設定完了</strong><br>
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li>テーブルコメント 20 件(Bronze 6 + Silver 6 + Gold 3 + 04 enrich 5)</li>
# MAGIC   <li>カラムコメント計 ~150 件(Silver / Gold 全 14 テーブル、業務名称・型・例・PK/FK 参照を含む)</li>
# MAGIC   <li>PK 14 個 / FK 19 個(Silver 6 + Gold MV 3 = 02 インライン、04 Delta 5 = 本 NB の ALTER TABLE)</li>
# MAGIC   <li>PII 列マスク 4 個(name / phone / vin / plate_number)</li>
# MAGIC </ul>
# MAGIC Catalog Explorer で <code>komae_demo_v4.used_car_e2e_demo</code> を開いて Lineage / Constraints / Mask が表示されるか確認してください。<br>
# MAGIC 次は <code>06_UC_Metrics_Views.py</code> で再利用可能なビジネス指標(KPI 定義)を Metric View として宣言します。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ トラブルシューティング</strong><br>
# MAGIC <b>"STREAMING_TABLE_OPERATION_NOT_ALLOWED.INVALID_ALTER"</b>: <code>ALTER TABLE</code> ではなく <code>ALTER STREAMING TABLE</code> を使う(本 NB は対応済み)<br>
# MAGIC <b>"Function ... not found"</b>: マスク関数の作成セルがスキップされている → Step 5 を順に実行<br>
# MAGIC <b>列マスクで "DROP MASK" が失敗</b>: 初回実行ではマスク未適用で正常エラー → <code>try/except</code> で吸収済み<br>
# MAGIC <b>PK/FK が情報スキーマに見えない</b>: 02 のパイプラインを再実行(<code>CREATE OR REFRESH</code> で初回のみインライン制約が反映される)
# MAGIC </div>
