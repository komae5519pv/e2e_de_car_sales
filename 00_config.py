# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 00 | 環境設定
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">中古車販売 E2E デモ</p>
# MAGIC     </div>
# MAGIC     <div style="margin-left: auto;">
# MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 5 min</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #FFC107; background: #FFF8E1; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🎯 このノートブックのゴール</strong><br>
# MAGIC 全ノートブック共通の <b>変数・スキーマ・Volume</b> をセットアップします。<br>
# MAGIC 他のノートブックの先頭で <code>%run ./00_config</code> を実行して使います。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,変数設定
MY_CATALOG  = "komae_demo_v4"          # カタログは事前作成済み前提
MY_SCHEMA   = "used_car_e2e_demo"      # このデモ専用スキーマ
MY_VOLUME   = "raw_data"               # CSV / PDF カタログを配置する Volume

VOLUME_PATH = f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}"

# AI Functions で使うモデル
LLM_MODEL = "databricks-claude-opus-4-7"

# Whisper 文字起こし用 Model Serving エンドポイント名
# UC 上の Whisper モデルを Model Serving にデプロイした際のエンドポイント名を指定
WHISPER_ENDPOINT = "komae_whisper_large_v3"

print(f"MY_CATALOG       : {MY_CATALOG}")
print(f"MY_SCHEMA        : {MY_SCHEMA}")
print(f"MY_VOLUME        : {MY_VOLUME}")
print(f"VOLUME_PATH      : {VOLUME_PATH}")
print(f"LLM_MODEL        : {LLM_MODEL}")
print(f"WHISPER_ENDPOINT : {WHISPER_ENDPOINT}")

# COMMAND ----------

# DBTITLE 1,リセット用（必要な場合のみコメント解除）
# spark.sql(f"DROP SCHEMA IF EXISTS {MY_CATALOG}.{MY_SCHEMA} CASCADE")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 🏗️ Unity Catalog セットアップ
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 Unity Catalog の3階層構造</strong><br>
# MAGIC <code>カタログ</code> &gt; <code>スキーマ</code> &gt; <code>テーブル / ビュー / Volume / 関数 / Metrics View / VS Index</code><br>
# MAGIC カタログは管理者が事前作成済みの前提です。ここではスキーマと Volume を作成します。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,スキーマ作成
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}")
print(f"✅ スキーマ '{MY_CATALOG}.{MY_SCHEMA}' を確認/作成しました")

# COMMAND ----------

# DBTITLE 1,Volume作成
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME}")
print(f"✅ Volume '{MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME}' を確認/作成しました")

# COMMAND ----------

# DBTITLE 1,デフォルトカタログ・スキーマ設定
# USE CATALOG / USE SCHEMA を実行しておくと、以降のSQLでカタログ・スキーマ名を省略できる
spark.sql(f"USE CATALOG {MY_CATALOG}")
spark.sql(f"USE SCHEMA {MY_SCHEMA}")
print(f"実行済み: USE CATALOG {MY_CATALOG}")
print(f"実行済み: USE SCHEMA {MY_SCHEMA}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ セットアップ完了</strong><br>
# MAGIC 他のノートブックの先頭で <code>%run ./00_config</code> を実行すると、<br>
# MAGIC <code>MY_CATALOG</code> / <code>MY_SCHEMA</code> / <code>MY_VOLUME</code> / <code>VOLUME_PATH</code> / <code>LLM_MODEL</code> が使えます。<br>
# MAGIC <code>USE CATALOG</code> / <code>USE SCHEMA</code> 設定済みのため、SQL でカタログ・スキーマ名の省略も可能です。
# MAGIC </div>
