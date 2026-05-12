-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # 02 | SDP パイプライン定義
-- MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
-- MAGIC   <div style="display: flex; align-items: center;">
-- MAGIC     <div>
-- MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">Bronze → Silver → Gold（Lakeflow Spark Declarative Pipelines / SQL）</p>
-- MAGIC     </div>
-- MAGIC     <div style="margin-left: auto;">
-- MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 25 min</span>
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
-- MAGIC <strong>⚠️ このノートブックの位置づけ</strong><br>
-- MAGIC これは <b>SDP（Lakeflow Spark Declarative Pipelines）に登録するソース定義ファイル</b>です。<br>
-- MAGIC ノートブックとして直接 Run しても何も起きません。次の <code>03_SDPパイプライン設定手順</code> の UI 操作で<br>
-- MAGIC <b>このファイルをパイプラインのソースとして登録 → 実行</b> します。
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
-- MAGIC <strong>📐 構成概要</strong><br>
-- MAGIC <b>Bronze（6 streaming tables）</b>：Auto Loader（<code>STREAM read_files()</code>）で Volume の CSV を増分取り込み。監査列 <code>_ingested_at</code> / <code>_source_file</code> を付与。<br>
-- MAGIC <b>Silver（6 streaming tables）</b>：Bronze をストリーム読みして型変換・クレンジング。<code>CONSTRAINT … EXPECT … ON VIOLATION</code> でデータ品質ルール、<code>PRIMARY KEY</code> / <code>FOREIGN KEY</code> でリレーションを宣言。<br>
-- MAGIC <b>Gold（3 materialized views）</b>：Silver から集計マートを構築。差分更新は SDP が自動判定。<br><br>
-- MAGIC <b>記法</b>：<code>CREATE OR REFRESH STREAMING TABLE</code> / <code>CREATE OR REFRESH MATERIALIZED VIEW</code>（Lakeflow SDP 構文。<code>CREATE OR REPLACE</code> や <code>CREATE LIVE TABLE</code> は古い書き方）。<br>
-- MAGIC <b>PK/FK について</b>：SDP では <code>ALTER TABLE</code> で後付けできないため、<b>CREATE 句のテーブル定義に直接記述</b>します。コメント・列マスクは 05 で <code>ALTER STREAMING TABLE</code> を使って後付けします。
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 🥉 Bronze 層
-- MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
-- MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">🥉 Bronze 層</h2>
-- MAGIC <p style="margin: 4px 0 0 0; color: #B0BEC5; font-size: 13px;">Volume の CSV を STREAM read_files() で増分取り込み</p>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 1/6: bz_stores
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 1/6: bz_stores</strong>（店舗マスタ）
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bz_stores
COMMENT '店舗マスタ Bronze。中古車プラザ / 買取専門 / 輸入車専門の 3 業態。'
AS SELECT
  *,
  current_timestamp()        AS _ingested_at,
  _metadata.file_path        AS _source_file
FROM STREAM read_files(
  '${volume_path}/master/',
  format          => 'csv',
  header          => 'true',
  pathGlobFilter  => 'stores.csv'
);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 2/6: bz_vehicles
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 2/6: bz_vehicles</strong>（車両在庫マスタ）
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bz_vehicles
COMMENT '車両在庫 Bronze。1,500 台、ボディタイプ 6 区分。'
AS SELECT
  *,
  current_timestamp()        AS _ingested_at,
  _metadata.file_path        AS _source_file
FROM STREAM read_files(
  '${volume_path}/master/',
  format          => 'csv',
  header          => 'true',
  pathGlobFilter  => 'vehicles.csv'
);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 3/6: bz_customers
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 3/6: bz_customers</strong>(顧客マスタ)
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bz_customers
COMMENT '顧客マスタ Bronze。氏名・電話番号は PII マスキング対象（後続 05_テーブル設定 で適用）。'
AS SELECT
  *,
  current_timestamp()        AS _ingested_at,
  _metadata.file_path        AS _source_file
FROM STREAM read_files(
  '${volume_path}/master/',
  format          => 'csv',
  header          => 'true',
  pathGlobFilter  => 'customers.csv'
);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 4/6: bz_market_index
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 4/6: bz_market_index</strong>(中古車相場指数)
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bz_market_index
COMMENT '中古車相場指数 Bronze。過去 2 年分の日次データ。'
AS SELECT
  *,
  current_timestamp()        AS _ingested_at,
  _metadata.file_path        AS _source_file
FROM STREAM read_files(
  '${volume_path}/master/',
  format          => 'csv',
  header          => 'true',
  pathGlobFilter  => 'market_index.csv'
);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 5/6: bz_inquiries
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 5/6: bz_inquiries</strong>(商談履歴・<b>日次 CSV を増分取り込み</b>)
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bz_inquiries
COMMENT '商談履歴 Bronze。履歴バルク + 直近 5 日分の日次 CSV を Auto Loader で増分取り込み。'
AS SELECT
  *,
  current_timestamp()        AS _ingested_at,
  _metadata.file_path        AS _source_file
FROM STREAM read_files(
  '${volume_path}/transactions/inquiries/',
  format          => 'csv',
  header          => 'true'
);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 6/6: bz_contracts
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 6/6: bz_contracts</strong>(成約・粗利・<b>日次 CSV を増分取り込み</b>)
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bz_contracts
COMMENT '成約・粗利 Bronze。履歴バルク + 直近 5 日分の日次 CSV を Auto Loader で増分取り込み。'
AS SELECT
  *,
  current_timestamp()        AS _ingested_at,
  _metadata.file_path        AS _source_file
FROM STREAM read_files(
  '${volume_path}/transactions/contracts/',
  format          => 'csv',
  header          => 'true'
);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 🥈 Silver 層
-- MAGIC <div style="background: #4E342E; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 24px 0 10px 0;">
-- MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">🥈 Silver 層</h2>
-- MAGIC <p style="margin: 4px 0 0 0; color: #BCAAA4; font-size: 13px;">Bronze をストリーム読み → 型変換・クレンジング・Expectations</p>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
-- MAGIC <strong>✅ Expectations について</strong><br>
-- MAGIC <code>CONSTRAINT &lt;name&gt; EXPECT (&lt;条件&gt;) ON VIOLATION &lt;DROP ROW | FAIL UPDATE&gt;</code> で品質ルールを宣言。<br>
-- MAGIC <b>DROP ROW</b>：違反レコードを除外して取り込みは継続 / <b>FAIL UPDATE</b>：違反があればパイプライン停止<br>
-- MAGIC SDP UI で <b>違反件数の自動メトリクス</b> が確認できます。
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 1/6: sl_stores
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 1/6: sl_stores</strong>
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sl_stores (
  store_id     STRING NOT NULL,
  store_name   STRING,
  format       STRING,
  prefecture   STRING,
  city         STRING,
  latitude     DOUBLE,
  longitude    DOUBLE,
  opened_date  DATE,
  CONSTRAINT valid_store_id   EXPECT (store_id IS NOT NULL)              ON VIOLATION DROP ROW,
  CONSTRAINT valid_format     EXPECT (format IN ('中古車プラザ','買取専門','輸入車専門')),
  CONSTRAINT pk_sl_stores PRIMARY KEY (store_id)
)
COMMENT '店舗マスタ Silver。Bronze からのクレンジング済み 30 店舗。PK: store_id。'
AS SELECT
  store_id,
  store_name,
  format,
  prefecture,
  city,
  CAST(latitude  AS DOUBLE) AS latitude,
  CAST(longitude AS DOUBLE) AS longitude,
  CAST(opened_date AS DATE) AS opened_date
FROM STREAM(bz_stores);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 2/6: sl_vehicles
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 2/6: sl_vehicles</strong>(複数の品質ルールを宣言)
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sl_vehicles (
  vehicle_id      STRING NOT NULL,
  maker           STRING,
  model           STRING,
  body_type       STRING,
  model_year      INT,
  mileage_km      INT,
  repair_history  STRING,
  certified       BOOLEAN,
  vin             STRING,
  plate_number    STRING,
  grade           STRING,
  purchase_price  BIGINT,
  listing_price   BIGINT,
  stock_in_date   DATE,
  status          STRING,
  store_id        STRING,
  CONSTRAINT valid_vehicle_id   EXPECT (vehicle_id IS NOT NULL)                       ON VIOLATION DROP ROW,
  CONSTRAINT model_year_in_range EXPECT (model_year BETWEEN 1990 AND 2027)            ON VIOLATION DROP ROW,
  CONSTRAINT vin_length_17       EXPECT (length(vin) = 17)                            ON VIOLATION DROP ROW,
  CONSTRAINT price_positive      EXPECT (purchase_price > 0 AND listing_price > 0)    ON VIOLATION DROP ROW,
  CONSTRAINT margin_non_negative EXPECT (listing_price >= purchase_price),
  CONSTRAINT pk_sl_vehicles  PRIMARY KEY (vehicle_id),
  CONSTRAINT fk_sl_vehicles_store FOREIGN KEY (store_id) REFERENCES sl_stores(store_id)
)
COMMENT '車両在庫 Silver。型変換 + Expectations（年式 / VIN 17 桁 / 販売価格 > 仕入価格）。PK: vehicle_id / FK: store_id。'
AS SELECT
  vehicle_id,
  maker,
  model,
  body_type,
  CAST(model_year   AS INT)    AS model_year,
  CAST(mileage_km   AS INT)    AS mileage_km,
  repair_history,
  CAST(certified    AS BOOLEAN) AS certified,
  vin,
  plate_number,
  grade,
  CAST(purchase_price AS BIGINT) AS purchase_price,
  CAST(listing_price  AS BIGINT) AS listing_price,
  CAST(stock_in_date  AS DATE)   AS stock_in_date,
  status,
  store_id
FROM STREAM(bz_vehicles);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 3/6: sl_customers
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 3/6: sl_customers</strong>
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sl_customers (
  customer_id           STRING NOT NULL,
  name                  STRING,
  age                   INT,
  gender                STRING,
  phone                 STRING,
  area                  STRING,
  registered_store_id   STRING,
  first_contact_date    DATE,
  annual_income_range   STRING,
  family_structure      STRING,
  lifestage             STRING,
  preferred_body_type   STRING,
  budget_max            BIGINT,
  CONSTRAINT valid_customer_id  EXPECT (customer_id IS NOT NULL)            ON VIOLATION DROP ROW,
  CONSTRAINT age_in_range       EXPECT (age BETWEEN 18 AND 100)             ON VIOLATION DROP ROW,
  CONSTRAINT valid_gender       EXPECT (gender IN ('M','F')),
  CONSTRAINT pk_sl_customers PRIMARY KEY (customer_id),
  CONSTRAINT fk_sl_customers_store FOREIGN KEY (registered_store_id) REFERENCES sl_stores(store_id)
)
COMMENT '顧客マスタ Silver。属性カラム（年収レンジ等）は一部 NULL のまま、04_NB 補完処理で ai_query を用いて enrich。PK: customer_id / FK: registered_store_id。'
AS SELECT
  customer_id,
  name,
  CAST(age AS INT)                            AS age,
  gender,
  phone,
  area,
  registered_store_id,
  CAST(first_contact_date AS DATE)            AS first_contact_date,
  annual_income_range,
  family_structure,
  lifestage,
  preferred_body_type,
  CAST(budget_max AS BIGINT)                  AS budget_max
FROM STREAM(bz_customers);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 4/6: sl_market_index
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 4/6: sl_market_index</strong>
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sl_market_index (
  date              DATE   NOT NULL,
  vehicle_category  STRING NOT NULL,
  market_index      DOUBLE,
  regular_gas_price DOUBLE,
  usd_jpy_rate      DOUBLE,
  CONSTRAINT valid_date          EXPECT (date IS NOT NULL)        ON VIOLATION DROP ROW,
  CONSTRAINT positive_index      EXPECT (market_index > 0)        ON VIOLATION DROP ROW,
  CONSTRAINT pk_sl_market_index PRIMARY KEY (date, vehicle_category)
)
COMMENT '中古車相場指数 Silver。複合 PK: (date, vehicle_category)。'
AS SELECT
  CAST(date AS DATE)                          AS date,
  vehicle_category,
  CAST(market_index       AS DOUBLE)          AS market_index,
  CAST(regular_gas_price  AS DOUBLE)          AS regular_gas_price,
  CAST(usd_jpy_rate       AS DOUBLE)          AS usd_jpy_rate
FROM STREAM(bz_market_index);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 5/6: sl_inquiries
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 5/6: sl_inquiries</strong>
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sl_inquiries (
  inquiry_id    STRING NOT NULL,
  customer_id   STRING,
  store_id      STRING,
  vehicle_id    STRING,
  inquiry_date  DATE,
  channel       STRING,
  status        STRING,
  inquiry_memo  STRING,
  CONSTRAINT valid_inquiry_id   EXPECT (inquiry_id IS NOT NULL)                            ON VIOLATION DROP ROW,
  CONSTRAINT valid_channel      EXPECT (channel IN ('来店','オンライン','買取査定','電話')),
  CONSTRAINT pk_sl_inquiries  PRIMARY KEY (inquiry_id),
  CONSTRAINT fk_sl_inquiries_customer FOREIGN KEY (customer_id) REFERENCES sl_customers(customer_id),
  CONSTRAINT fk_sl_inquiries_store    FOREIGN KEY (store_id)    REFERENCES sl_stores(store_id),
  CONSTRAINT fk_sl_inquiries_vehicle  FOREIGN KEY (vehicle_id)  REFERENCES sl_vehicles(vehicle_id)
)
COMMENT '商談履歴 Silver。inquiry_memo は 04_NB 補完処理で ai_query / ai_classify の入力に使う。PK: inquiry_id / FK: customer_id, store_id, vehicle_id。'
AS SELECT
  inquiry_id,
  customer_id,
  store_id,
  vehicle_id,
  CAST(inquiry_date AS DATE) AS inquiry_date,
  channel,
  status,
  inquiry_memo
FROM STREAM(bz_inquiries);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 6/6: sl_contracts
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 6/6: sl_contracts</strong>(粗利整合性チェック)
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sl_contracts (
  contract_id     STRING NOT NULL,
  inquiry_id      STRING,
  customer_id     STRING,
  store_id        STRING,
  vehicle_id      STRING,
  contract_date   DATE,
  listing_price   BIGINT,
  actual_price    BIGINT,
  purchase_price  BIGINT,
  gross_profit    BIGINT,
  gross_margin    DOUBLE,
  payment_method  STRING,
  CONSTRAINT valid_contract_id   EXPECT (contract_id IS NOT NULL)                                          ON VIOLATION DROP ROW,
  CONSTRAINT positive_actual     EXPECT (actual_price > 0)                                                 ON VIOLATION DROP ROW,
  CONSTRAINT margin_consistency  EXPECT (abs(gross_profit - (actual_price - purchase_price)) < 1),
  CONSTRAINT pk_sl_contracts  PRIMARY KEY (contract_id),
  CONSTRAINT fk_sl_contracts_inquiry  FOREIGN KEY (inquiry_id)  REFERENCES sl_inquiries(inquiry_id),
  CONSTRAINT fk_sl_contracts_customer FOREIGN KEY (customer_id) REFERENCES sl_customers(customer_id),
  CONSTRAINT fk_sl_contracts_store    FOREIGN KEY (store_id)    REFERENCES sl_stores(store_id),
  CONSTRAINT fk_sl_contracts_vehicle  FOREIGN KEY (vehicle_id)  REFERENCES sl_vehicles(vehicle_id)
)
COMMENT '成約・粗利 Silver。粗利の整合性を Expectation でチェック。PK: contract_id / FK: inquiry_id, customer_id, store_id, vehicle_id。'
AS SELECT
  contract_id,
  inquiry_id,
  customer_id,
  store_id,
  vehicle_id,
  CAST(contract_date  AS DATE)   AS contract_date,
  CAST(listing_price  AS BIGINT) AS listing_price,
  CAST(actual_price   AS BIGINT) AS actual_price,
  CAST(purchase_price AS BIGINT) AS purchase_price,
  CAST(gross_profit   AS BIGINT) AS gross_profit,
  CAST(gross_margin   AS DOUBLE) AS gross_margin,
  payment_method
FROM STREAM(bz_contracts);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 🥇 Gold 層
-- MAGIC <div style="background: #B8860B; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 24px 0 10px 0;">
-- MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">🥇 Gold 層</h2>
-- MAGIC <p style="margin: 4px 0 0 0; color: #FFE082; font-size: 13px;">Silver から集計マートを構築（Materialized View / 差分更新は SDP が自動判定）</p>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 1/3: gd_store_monthly_sales
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 1/3: gd_store_monthly_sales</strong>(店舗 × 月別 販売台数・粗利)
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gd_store_monthly_sales (
  store_id            STRING    NOT NULL,
  store_name          STRING,
  prefecture          STRING,
  format              STRING,
  sales_month         TIMESTAMP NOT NULL,
  contracts_count     BIGINT,
  sales_amount        BIGINT,
  gross_profit_total  BIGINT,
  avg_gross_margin    DOUBLE,
  CONSTRAINT pk_gd_store_monthly_sales PRIMARY KEY (store_id, sales_month),
  CONSTRAINT fk_gd_store_monthly_sales_store FOREIGN KEY (store_id) REFERENCES sl_stores(store_id)
)
COMMENT '店舗 × 月別の販売台数・売上・粗利集計マート（ダッシュボード / Genie の主軸）。PK: (store_id, sales_month) / FK: store_id。'
AS SELECT
  c.store_id,
  s.store_name,
  s.prefecture,
  s.format,
  date_trunc('month', c.contract_date)  AS sales_month,
  COUNT(*)                              AS contracts_count,
  SUM(c.actual_price)                   AS sales_amount,
  SUM(c.gross_profit)                   AS gross_profit_total,
  AVG(c.gross_margin)                   AS avg_gross_margin
FROM sl_contracts c
JOIN sl_stores   s USING (store_id)
GROUP BY c.store_id, s.store_name, s.prefecture, s.format, date_trunc('month', c.contract_date);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 2/3: gd_vehicle_inventory
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 2/3: gd_vehicle_inventory</strong>(ボディタイプ別在庫状況)
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gd_vehicle_inventory (
  body_type             STRING NOT NULL,
  status                STRING NOT NULL,
  vehicles_count        BIGINT,
  avg_listing_price     DOUBLE,
  avg_mileage_km        DOUBLE,
  repair_history_count  BIGINT,
  CONSTRAINT pk_gd_vehicle_inventory PRIMARY KEY (body_type, status)
)
COMMENT 'ボディタイプ別の在庫状況サマリ（在庫中 / 商談中 / 成約済 / 出庫済）。PK: (body_type, status)。'
AS SELECT
  body_type,
  status,
  COUNT(*)                              AS vehicles_count,
  AVG(listing_price)                    AS avg_listing_price,
  AVG(mileage_km)                       AS avg_mileage_km,
  SUM(CASE WHEN repair_history = 'あり' THEN 1 ELSE 0 END) AS repair_history_count
FROM sl_vehicles
GROUP BY body_type, status;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 3/3: gd_market_linked_margin
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 3/3: gd_market_linked_margin</strong>(相場 × 粗利率の月次比較)
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gd_market_linked_margin (
  month             TIMESTAMP NOT NULL,
  vehicle_category  STRING    NOT NULL,
  avg_market_index  DOUBLE,
  avg_usd_jpy       DOUBLE,
  contracts_count   BIGINT,
  avg_gross_margin  DOUBLE,
  CONSTRAINT pk_gd_market_linked_margin PRIMARY KEY (month, vehicle_category)
)
COMMENT '中古車相場指数 × 販売粗利率の月次比較。相場連動分析の入力。PK: (month, vehicle_category)。'
AS
WITH monthly_market AS (
  SELECT
    date_trunc('month', date)             AS month,
    vehicle_category,
    AVG(market_index)                     AS avg_market_index,
    AVG(usd_jpy_rate)                     AS avg_usd_jpy
  FROM sl_market_index
  GROUP BY date_trunc('month', date), vehicle_category
),
monthly_sales AS (
  SELECT
    date_trunc('month', c.contract_date)  AS month,
    v.body_type                            AS vehicle_category,
    COUNT(*)                              AS contracts_count,
    AVG(c.gross_margin)                   AS avg_gross_margin
  FROM sl_contracts c
  JOIN sl_vehicles v USING (vehicle_id)
  GROUP BY date_trunc('month', c.contract_date), v.body_type
)
SELECT
  m.month,
  m.vehicle_category,
  m.avg_market_index,
  m.avg_usd_jpy,
  s.contracts_count,
  s.avg_gross_margin
FROM monthly_market m
LEFT JOIN monthly_sales s
  ON m.month = s.month AND m.vehicle_category = s.vehicle_category;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
-- MAGIC <strong>✅ 定義完了</strong><br>
-- MAGIC Bronze 6 + Silver 6 + Gold 3 = <b>15 オブジェクト</b> を SDP として宣言しました。<br>
-- MAGIC 次は <code>03_SDPパイプライン設定手順</code> の手順に従って、Databricks UI からこのファイルをソースに指定して<br>
-- MAGIC <b>パイプラインを作成 → 実行</b> します。実行すると DAG（依存関係グラフ）と Expectations の違反件数が UI で確認できます。
-- MAGIC </div>
