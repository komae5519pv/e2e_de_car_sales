# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 04 | AI による非構造データ処理(音声 + テキスト + PDF)
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">音声 → 文字起こし → AI で構造化、PDF をパース、テキストを enrich</p>
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
# MAGIC <b>商談録音(MP3) / 自由記述メモ / 車両カタログ PDF</b> といった非構造データを、<br>
# MAGIC <b>Databricks AI Functions</b>(<code>ai_query</code> + Whisper / <code>ai_classify</code> / <code>ai_extract</code> / <code>ai_parse_document</code>) で<br>
# MAGIC <b>分析可能な構造化テーブル</b>に変換します。<br>
# MAGIC <br>
# MAGIC <b>生成テーブル(6 個):</b>
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li><code>sl_inquiry_audio_transcribed</code> - <b>商談録音 MP3 を Whisper で文字起こし</b></li>
# MAGIC   <li><code>sl_inquiries_enriched</code> - 引合カテゴリ自動分類 + 希望条件抽出</li>
# MAGIC   <li><code>sl_customers_enriched</code> - NULL 属性を ai_query で推論補完</li>
# MAGIC   <li><code>sl_pdf_catalogs_parsed</code> - 車両カタログ PDF をパース(後続 RAG の入力)</li>
# MAGIC   <li><code>gd_contract_margin_score</code> - NTILE による粗利率 5 段階スコア</li>
# MAGIC   <li><code>gd_customer_rm_segment</code> - 顧客 RM セグメンテーション</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📋 前提条件</strong><br>
# MAGIC ✅ <code>01_データ準備.py</code> 実行済み(<code>{VOLUME_PATH}/audio/</code> に MP3 配置済み)<br>
# MAGIC ✅ <code>03_SDPパイプライン設定手順.py</code> 実行済み(<code>sl_*</code> / <code>gd_*</code> テーブル作成済み)<br>
# MAGIC ✅ <b>Whisper Model Serving エンドポイント</b>が稼働中(<code>00_config</code> の <code>WHISPER_ENDPOINT</code>)<br>
# MAGIC ✅ サーバーレス SQL ウェアハウス または DBR 17.1+ クラスタ(<code>ai_parse_document</code> 利用のため)<br>
# MAGIC ✅ ワークスペースが AI Functions 対応リージョン
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💰 コストに関する注意</strong><br>
# MAGIC AI Functions は内部で <b>Foundation Model API(Pay-per-token)</b> を呼びます。<br>
# MAGIC 本ノートブックではデモ目的で <b><code>LIMIT 100</code> など処理件数を絞った例</b>のみを示します。<br>
# MAGIC 本番運用では Bronze 層全件を一気に処理せず、<b>差分のみ</b>を AI Functions に流すのが定石です。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,共通設定の読み込み
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 1. ai_query + Whisper - 商談録音(MP3)を文字起こし
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 1. ai_query + Whisper - 商談録音(MP3)を文字起こし</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC 01 で Volume <code>{VOLUME_PATH}/audio/</code> に配置した <b>商談録音 MP3</b> を、<br>
# MAGIC UC 上の <b>Whisper モデル</b>(Model Serving エンドポイント化済み)で文字起こしします。<br>
# MAGIC <b>使うべき関数</b>: <code>ai_query('{WHISPER_ENDPOINT}', content)</code> -- バイナリの音声を渡すと文字起こし結果が返る。<br>
# MAGIC ➡ 文字起こし結果は後続の <code>ai_classify</code> / <code>ai_extract</code> で構造化(Step 2-3)できます。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ 前準備: Whisper を Model Serving にデプロイ</strong><br>
# MAGIC UC に登録済みの Whisper モデルを「<b>Serving</b>」画面から <b>Model Serving エンドポイント</b>として手動デプロイしてください。<br>
# MAGIC エンドポイント名は <code>00_config</code> の <code>WHISPER_ENDPOINT</code> と一致させること。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Volume の音声ファイル一覧を確認
display(
    dbutils.fs.ls(f"{VOLUME_PATH}/audio/")
)

# COMMAND ----------

# DBTITLE 1,MP3 をバイナリで読込 → ai_query(Whisper) で文字起こし → Delta に保存
spark.sql(f"""
CREATE OR REPLACE TABLE sl_inquiry_audio_transcribed
COMMENT '商談録音(MP3)を Whisper で文字起こしした接客テキスト。後続の ai_classify / ai_extract の入力。'
AS
SELECT
  regexp_extract(path, '([^/]+)\\\\.mp3$', 1)  AS audio_file_name,
  ai_query('{WHISPER_ENDPOINT}', content)    AS transcript,
  current_timestamp()                        AS _ingested_at
FROM read_files(
  '{VOLUME_PATH}/audio/',
  format => 'binaryFile'
)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 文字起こし結果を確認
# MAGIC SELECT
# MAGIC   audio_file_name,
# MAGIC   substring(transcript, 1, 200) AS transcript_preview,
# MAGIC   length(transcript)            AS transcript_length
# MAGIC FROM sl_inquiry_audio_transcribed
# MAGIC ORDER BY audio_file_name;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 ポイント: 音声 → テキスト → 構造化までを SQL 1 本で繋げる</strong><br>
# MAGIC <code>ai_query</code> は Foundation Model API だけでなく <b>Model Serving エンドポイント全般</b>に対応しています。<br>
# MAGIC ここでは Whisper(音声→テキスト)を呼び出し、後続の <code>ai_classify</code> / <code>ai_extract</code> で<br>
# MAGIC <b>「商談録音から引合カテゴリ・希望条件を自動抽出」</b>するパイプラインを <b>SQL だけで完結</b>できます。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 2. ai_classify - 引合カテゴリの自動分類
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 2. ai_classify - 引合カテゴリの自動分類</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <code>sl_inquiries.inquiry_memo</code>(自由記述の商談メモ)を、固定ラベル群に分類します。<br>
# MAGIC <b>分類ラベル</b>: <code>購入検討</code> / <code>値引交渉</code> / <code>買取査定</code> / <code>在庫確認</code> / <code>その他</code><br>
# MAGIC <b>使うべき関数</b>: <code>ai_classify(text, ARRAY(...))</code> -- ラベル数 2-500 個まで対応、ラベルが固定なら <code>ai_query</code> より高速・高精度。
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- inquiry_memo の中身を 5 件確認
# MAGIC SELECT inquiry_id, channel, status, inquiry_memo
# MAGIC FROM sl_inquiries
# MAGIC WHERE inquiry_memo IS NOT NULL
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ai_classify で引合カテゴリを推論しテーブル化(LIMIT 100 でコスト制御)
# MAGIC CREATE OR REPLACE TABLE sl_inquiries_enriched
# MAGIC COMMENT 'sl_inquiries に AI 分類ラベル(inquiry_category)を付与した enrich テーブル'
# MAGIC AS
# MAGIC SELECT
# MAGIC   inquiry_id,
# MAGIC   customer_id,
# MAGIC   store_id,
# MAGIC   vehicle_id,
# MAGIC   inquiry_date,
# MAGIC   channel,
# MAGIC   status,
# MAGIC   inquiry_memo,
# MAGIC   ai_classify(
# MAGIC     inquiry_memo,
# MAGIC     ARRAY('購入検討', '値引交渉', '買取査定', '在庫確認', 'その他')
# MAGIC   ) AS inquiry_category
# MAGIC FROM sl_inquiries
# MAGIC WHERE inquiry_memo IS NOT NULL
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 分類結果のサマリ
# MAGIC SELECT inquiry_category, COUNT(*) AS cnt
# MAGIC FROM sl_inquiries_enriched
# MAGIC GROUP BY inquiry_category
# MAGIC ORDER BY cnt DESC;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 3. ai_extract - 希望条件の構造化抽出
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 3. ai_extract - 希望条件の構造化抽出</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <code>inquiry_memo</code> から <b>希望ボディタイプ</b> / <b>予算上限</b> / <b>希望メーカー</b>などのフィールドを STRUCT で取り出します。<br>
# MAGIC <b>使うべき関数</b>: <code>ai_extract(text, ARRAY('field1', 'field2', ...))</code><br>
# MAGIC ➡ 抽出結果は <code>STRUCT&lt;field1: STRING, field2: STRING, ...&gt;</code> として返るので、<code>.field1</code> でアクセス可能。
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ai_extract で希望条件を抽出 → enriched テーブルにマージ
# MAGIC CREATE OR REPLACE TABLE sl_inquiries_enriched AS
# MAGIC SELECT
# MAGIC   e.*,
# MAGIC   ai_extract(
# MAGIC     e.inquiry_memo,
# MAGIC     ARRAY('preferred_body_type', 'budget_max_yen', 'preferred_maker')
# MAGIC   ) AS extracted_conditions
# MAGIC FROM sl_inquiries_enriched e;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 抽出結果(STRUCT)からフィールドを取り出し
# MAGIC SELECT
# MAGIC   inquiry_id,
# MAGIC   inquiry_memo,
# MAGIC   inquiry_category,
# MAGIC   extracted_conditions.preferred_body_type AS preferred_body_type,
# MAGIC   extracted_conditions.budget_max_yen      AS budget_max_yen,
# MAGIC   extracted_conditions.preferred_maker     AS preferred_maker
# MAGIC FROM sl_inquiries_enriched
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 4. ai_query - 顧客属性(NULL)を推論で補完
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 4. ai_query - 顧客属性(NULL)を推論で補完</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <code>sl_customers</code> には、<code>annual_income_range</code> / <code>family_structure</code> / <code>lifestage</code> /
# MAGIC <code>preferred_body_type</code> / <code>budget_max</code> が <b>意図的に NULL</b> な顧客が約 45% 含まれています(01 で生成)。<br>
# MAGIC これを、<b>その顧客の inquiry_memo を全件結合 → ai_query で 5 項目を JSON として推論</b>して埋めます。<br>
# MAGIC <b>使うべき関数</b>: <code>ai_query('databricks-claude-opus-4-7', prompt, responseFormat=&gt;'{"type":"json_object"}')</code><br>
# MAGIC ➡ 構造化 JSON が必要なときは <code>ai_extract</code> で済まないので <code>ai_query</code> を使う(タスク特化関数 → 汎用の順で選択)。<br>
# MAGIC ※ <code>ai_query</code> のバッチ推論は <b>Llama / Claude Sonnet / GPT-OSS</b> 等が対応(Opus は非対応)。ここでは <code>databricks-meta-llama-3-3-70b-instruct</code> を使用。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ コスト注意</strong>: 顧客 1 人につき LLM 呼出 1 回。<br>
# MAGIC デモでは <b>NULL 属性顧客のうち 30 名のみ</b>を対象にします(<code>WHERE annual_income_range IS NULL LIMIT 30</code>)。
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 各顧客の inquiry_memo を集約 → ai_query で属性を推論 → JSON で受ける
# MAGIC CREATE OR REPLACE TABLE sl_customers_enriched AS
# MAGIC WITH null_customers AS (
# MAGIC   SELECT customer_id
# MAGIC   FROM sl_customers
# MAGIC   WHERE annual_income_range IS NULL
# MAGIC   LIMIT 30
# MAGIC ),
# MAGIC customer_memos AS (
# MAGIC   SELECT
# MAGIC     i.customer_id,
# MAGIC     concat_ws(' / ', collect_list(i.inquiry_memo)) AS memo_concat
# MAGIC   FROM sl_inquiries i
# MAGIC   JOIN null_customers n USING (customer_id)
# MAGIC   WHERE i.inquiry_memo IS NOT NULL
# MAGIC   GROUP BY i.customer_id
# MAGIC ),
# MAGIC enriched AS (
# MAGIC   SELECT
# MAGIC     m.customer_id,
# MAGIC     m.memo_concat,
# MAGIC     ai_query(
# MAGIC       'databricks-meta-llama-3-3-70b-instruct',
# MAGIC       concat(
# MAGIC         '次の中古車店での顧客の商談メモから、その顧客の属性を JSON で推論してください。',
# MAGIC         '出力フィールド: annual_income_range(〜400万/400-600万/600-800万/800-1000万/1000万〜), ',
# MAGIC         'family_structure(独身/夫婦のみ/子育て世代/子育て後/シニア), ',
# MAGIC         'lifestage(新社会人/結婚適齢期/子育て世代/子育て後/シニア), ',
# MAGIC         'preferred_body_type(軽自動車/コンパクト/セダン/SUV/ミニバン/輸入車), ',
# MAGIC         'budget_max(整数・円). 確信が持てないフィールドは null を入れてください。',
# MAGIC         '商談メモ: ', m.memo_concat
# MAGIC       ),
# MAGIC       responseFormat => '{"type":"json_object"}',
# MAGIC       failOnError    => false
# MAGIC     ) AS ai_response
# MAGIC   FROM customer_memos m
# MAGIC )
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   memo_concat,
# MAGIC   ai_response.result                                                                              AS ai_json,
# MAGIC   from_json(ai_response.result,
# MAGIC     'STRUCT<annual_income_range:STRING, family_structure:STRING, lifestage:STRING, preferred_body_type:STRING, budget_max:BIGINT>'
# MAGIC   ) AS inferred
# MAGIC FROM enriched;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 推論結果を確認
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   inferred.annual_income_range,
# MAGIC   inferred.family_structure,
# MAGIC   inferred.lifestage,
# MAGIC   inferred.preferred_body_type,
# MAGIC   inferred.budget_max,
# MAGIC   substring(memo_concat, 1, 80) AS memo_sample
# MAGIC FROM sl_customers_enriched
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 5. ai_parse_document - 車両カタログ PDF を構造化
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 5. ai_parse_document - 車両カタログ PDF を構造化</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC 01 で Volume <code>{VOLUME_PATH}/pdf_catalogs/</code> に配置した
# MAGIC <b>5 車種の PDF カタログ</b>(ハリアー / ヴェゼル / シエンタ / ノート / N-BOX)を <code>ai_parse_document</code> でパースし、
# MAGIC <b>テキスト + メタデータ</b>を Delta テーブルに格納します。<br>
# MAGIC ➡ このテーブルは <code>07_RAG構築.py</code> で <b>Vector Search の元データ</b>として使います。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,処理対象の PDF Volume パス
displayHTML(f"<p>📁 PDF カタログの Volume パス: <code>{VOLUME_PATH}/pdf_catalogs/</code></p>")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ DBR 要件</strong>: <code>ai_parse_document</code> は <b>DBR 17.1+ サーバーレス</b>または対応 SQL Warehouse でのみ動作。<br>
# MAGIC エラーが出たら、ノートブックのコンピュートを「Serverless」に切り替えてください。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Volume の PDF を一覧確認
display(
    dbutils.fs.ls(f"{VOLUME_PATH}/pdf_catalogs/")
)

# COMMAND ----------

# DBTITLE 1,PDF をバイナリで読込 → ai_parse_document でパース → Delta に保存
# Volume パスを VOLUME_PATH から動的に組み立てるため、SQL ではなく spark.sql(f"...") で実行
spark.sql(f"""
CREATE OR REPLACE TABLE sl_pdf_catalogs_parsed
COMMENT '車両カタログ PDF を ai_parse_document でパース。後続 RAG の入力。'
AS
SELECT
  path,
  regexp_extract(path, '([^/]+)\\\\.pdf$', 1) AS catalog_name,
  ai_parse_document(content)                AS parsed,
  current_timestamp()                       AS _ingested_at
FROM read_files(
  '{VOLUME_PATH}/pdf_catalogs/',
  format => 'binaryFile'
)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- パース結果の構造を確認(ai_parse_document の返り値を JSON 文字列化して先頭 800 文字)
# MAGIC SELECT
# MAGIC   catalog_name,
# MAGIC   substring(to_json(parsed), 1, 800) AS parsed_preview
# MAGIC FROM sl_pdf_catalogs_parsed;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 6. NTILE で粗利率を 5 段階スコア化
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 6. NTILE で粗利率を 5 段階スコア化</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <b>SDP の MV では書きづらい</b>ウィンドウ関数を notebook で書きます。<br>
# MAGIC 各成約契約に対して、<b>店舗別の粗利率の分布で 5 段階(1=最低 - 5=最高)</b>のスコアを付与します。<br>
# MAGIC ダッシュボードで「店舗内で粗利が高い・低い案件はどれか」を可視化する用途。
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gd_contract_margin_score
# MAGIC COMMENT '契約ごとの粗利率を店舗別に NTILE(5) でスコア化(1=低 〜 5=高)。'
# MAGIC AS
# MAGIC SELECT
# MAGIC   contract_id,
# MAGIC   store_id,
# MAGIC   vehicle_id,
# MAGIC   customer_id,
# MAGIC   contract_date,
# MAGIC   actual_price,
# MAGIC   gross_profit,
# MAGIC   gross_margin,
# MAGIC   NTILE(5) OVER (PARTITION BY store_id ORDER BY gross_margin) AS margin_score
# MAGIC FROM sl_contracts;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- スコア分布を確認(各店舗で 1-5 が均等に出る想定)
# MAGIC SELECT margin_score, COUNT(*) AS cnt, ROUND(AVG(gross_margin), 4) AS avg_margin
# MAGIC FROM gd_contract_margin_score
# MAGIC GROUP BY margin_score
# MAGIC ORDER BY margin_score;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 7. 顧客 RM セグメンテーション
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 7. 顧客 RM セグメンテーション</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC 中古車は購買頻度が極小(生涯 2-5 回)なので、典型的な RFM ではなく <b>RM(Recency × Monetary)</b> で 4 セグメントに分類します。<br>
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li><b>VIP</b>: 最終接点 ≦ 12 ヶ月 かつ 累計取引額 上位 30%</li>
# MAGIC   <li><b>Loyal</b>: 最終接点 ≦ 12 ヶ月 かつ 累計取引額 中位/下位</li>
# MAGIC   <li><b>At Risk</b>: 最終接点 &gt; 12 ヶ月 かつ 累計取引額 上位 30%</li>
# MAGIC   <li><b>Lost</b>: 最終接点 &gt; 12 ヶ月 かつ 累計取引額 中位/下位</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gd_customer_rm_segment
# MAGIC COMMENT '顧客 RM(Recency × Monetary)セグメント。VIP/Loyal/At Risk/Lost の 4 区分。'
# MAGIC AS
# MAGIC WITH rm_base AS (
# MAGIC   SELECT
# MAGIC     c.customer_id,
# MAGIC     MAX(ct.contract_date)                                      AS last_contract_date,
# MAGIC     COALESCE(SUM(ct.actual_price), 0)                          AS total_spend,
# MAGIC     months_between(current_date(), MAX(ct.contract_date))      AS recency_months
# MAGIC   FROM sl_customers c
# MAGIC   LEFT JOIN sl_contracts ct USING (customer_id)
# MAGIC   GROUP BY c.customer_id
# MAGIC ),
# MAGIC scored AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     PERCENT_RANK() OVER (ORDER BY total_spend) AS spend_pct
# MAGIC   FROM rm_base
# MAGIC )
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   last_contract_date,
# MAGIC   total_spend,
# MAGIC   recency_months,
# MAGIC   CASE
# MAGIC     WHEN recency_months IS NULL                       THEN 'Prospect'
# MAGIC     WHEN recency_months <= 12 AND spend_pct >= 0.7    THEN 'VIP'
# MAGIC     WHEN recency_months <= 12                         THEN 'Loyal'
# MAGIC     WHEN recency_months >  12 AND spend_pct >= 0.7    THEN 'At Risk'
# MAGIC     ELSE                                                   'Lost'
# MAGIC   END AS rm_segment
# MAGIC FROM scored;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- セグメント分布を確認
# MAGIC SELECT rm_segment, COUNT(*) AS customers, ROUND(AVG(total_spend), 0) AS avg_spend
# MAGIC FROM gd_customer_rm_segment
# MAGIC GROUP BY rm_segment
# MAGIC ORDER BY avg_spend DESC;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 8. 結果まとめ
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 8. 結果まとめ</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 04 で生成した 6 テーブルの行数一覧
# MAGIC SELECT 'sl_inquiry_audio_transcribed' AS table_name, COUNT(*) AS row_count FROM sl_inquiry_audio_transcribed UNION ALL
# MAGIC SELECT 'sl_inquiries_enriched',        COUNT(*) FROM sl_inquiries_enriched                                   UNION ALL
# MAGIC SELECT 'sl_customers_enriched',        COUNT(*) FROM sl_customers_enriched                                   UNION ALL
# MAGIC SELECT 'sl_pdf_catalogs_parsed',       COUNT(*) FROM sl_pdf_catalogs_parsed                                  UNION ALL
# MAGIC SELECT 'gd_contract_margin_score',     COUNT(*) FROM gd_contract_margin_score                                UNION ALL
# MAGIC SELECT 'gd_customer_rm_segment',       COUNT(*) FROM gd_customer_rm_segment
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ AI による非構造データ処理 完了</strong><br>
# MAGIC <b>音声 → テキスト → 構造化</b>(Whisper + ai_classify + ai_extract)、<br>
# MAGIC <b>顧客属性の推論補完</b>(ai_query)、<b>PDF カタログのパース</b>(ai_parse_document)、<br>
# MAGIC <b>NTILE スコアリング</b>・<b>RM セグメンテーション</b>を SQL ベースで完結させました。<br>
# MAGIC 次は <code>05_テーブル設定.py</code> で全テーブルにコメント・PK/FK・PII マスキングを一括適用します。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ トラブルシューティング</strong><br>
# MAGIC <b>Whisper エンドポイントに接続できない</b>: Serving 画面でエンドポイントが <code>READY</code> か確認 / <code>WHISPER_ENDPOINT</code> 名が一致しているか<br>
# MAGIC <b>音声の文字起こしが空</b>: MP3 が <code>{VOLUME_PATH}/audio/</code> に配置されているか確認(01 で配置済み)<br>
# MAGIC <b><code>ai_parse_document</code> not found</b>: コンピュートを Serverless または DBR 17.1+ に切り替え<br>
# MAGIC <b><code>ai_query</code> が一部 NULL を返す</b>: <code>failOnError =&gt; false</code> 指定済み。<code>ai_response.errorMessage</code> を確認<br>
# MAGIC <b>すべて NULL</b>: 入力カラムが NULL → <code>WHERE col IS NOT NULL</code> でフィルタしてから呼出<br>
# MAGIC <b>処理が遅い</b>: バッチ用には DBR 15.4 ML LTS を選ぶと AI Functions スループットが最適化される
# MAGIC </div>
