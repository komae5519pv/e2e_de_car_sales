# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 03 | SDP パイプライン設定手順
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">Databricks UI で 02 のソースを登録 → 実行 → 結果確認</p>
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
# MAGIC <code>02_SDPパイプライン定義.py</code> をソースに指定して <b>Lakeflow パイプライン</b>を作成し、<br>
# MAGIC 実行 → DAG・Expectations 違反件数・データ系統（Lineage）を UI 上で確認します。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,共通設定の読み込み
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📋 前提条件</strong><br>
# MAGIC ✅ <code>00_config.py</code> 実行済み(スキーマ + Volume 作成済み)<br>
# MAGIC ✅ <code>01_データ準備.py</code> 実行済み(Volume に CSV / PDF 配置済み)<br>
# MAGIC ✅ <code>02_SDPパイプライン定義.py</code> をワークスペースにアップロード済み
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,このノートブックで使うカタログ・スキーマ(00_config から取得)
from IPython.display import Markdown
displayHTML(f"""
<div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
<strong>🎯 パイプライン作成時に UI に入力する値(00_config から取得)</strong><br>
<table style="margin-top: 8px;">
  <tr><td><b>デフォルトカタログ</b></td><td><code>{MY_CATALOG}</code></td></tr>
  <tr><td><b>デフォルトスキーマ</b></td><td><code>{MY_SCHEMA}</code></td></tr>
  <tr><td><b>Volume パス</b></td><td><code>{VOLUME_PATH}</code></td></tr>
</table>
<i>※ 値を変えたい場合は <code>00_config.py</code> の <code>MY_CATALOG</code> / <code>MY_SCHEMA</code> を編集してください。</i>
</div>
""")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🗺️ 全体の流れ</strong><br>
# MAGIC <pre style="background: #FFFFFF; padding: 10px; border-radius: 4px; margin: 8px 0; font-size: 12px; line-height: 1.6;">
# MAGIC Jobs & Pipelines → Create → ETL pipeline
# MAGIC     ↓
# MAGIC <b>空のパイプラインが即作成されます</b>（作成ダイアログ無し）
# MAGIC     ↓
# MAGIC 右上の <b>「設定」ボタン</b> → <b>「パイプライン設定」ペイン</b>を開いて以下を設定：
# MAGIC   ① パイプライン名・パイプラインモード
# MAGIC   ② コードアセット（ルートフォルダ + ソースコード）  ← 「パスを構成」ボタンでサブペイン
# MAGIC   ③ データアセットのデフォルトの格納場所（カタログ・スキーマ）  ← 「カタログとスキーマを編集」ボタン
# MAGIC   ④ コンピュート（サーバーレスを確認）
# MAGIC     ↓
# MAGIC （任意）右上の <b>「スケジュール」ボタン</b> で定期自動実行
# MAGIC     ↓
# MAGIC （任意）右上の <b>「ドライラン」ボタン</b> で定義のみ検証（〜1 分）
# MAGIC     ↓
# MAGIC 右上の <b>「パイプラインを実行」ボタン</b> → 初回実行（5〜8 分）
# MAGIC     ↓
# MAGIC DAG / Expectations / Lineage を UI 上で確認
# MAGIC </pre>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 1. 空のパイプラインを作成
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 1. 空のパイプラインを作成</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC 左サイドバー <b>Jobs &amp; Pipelines</b> → 右上の <b>「Create」ボタン</b> → <b>「ETL pipeline」</b> を選択。<br>
# MAGIC ➡ <b>空のパイプラインが即作成され、パイプライン詳細画面に遷移</b>します。<br>
# MAGIC （作成ダイアログは表示されません。設定はこの後、「設定」ボタンから行います）
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ 自動生成されるテンプレートフォルダについて</strong><br>
# MAGIC Create → ETL pipeline を押すと、Databricks がワークスペース上に <b>パイプライン用テンプレートフォルダ</b><br>
# MAGIC （例：<code>/Workspace/Users/&lt;your-email&gt;/&lt;pipeline_name&gt;/transformations/</code> 等にサンプル SQL/Python ファイル）を自動生成します。<br>
# MAGIC 今回は <b>既に <code>02_SDPパイプライン定義.py</code> を別フォルダに用意している</b> ため、<br>
# MAGIC この自動生成フォルダは <b>不要</b>です。Step 3 のソースコード指定で 02 のノートブックを直接指してから、<br>
# MAGIC 自動生成された <code>transformations/</code> フォルダごと削除して構いません（パイプライン本体は残ります）。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 2. 「設定」ボタン →「パイプライン設定」ペインを開く
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 2. 「設定」ボタン →「パイプライン設定」ペインを開く</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC パイプライン詳細画面の <b>右上「設定」ボタン</b> をクリック。<br>
# MAGIC ➡ 右側に <b>「パイプライン設定」ペイン</b> が開きます。<br>
# MAGIC ペイン上部で <b>UI / JSON / YAML</b> の表示形式を切り替え可能。以下では <b>UI</b> 形式で進めます。<br>
# MAGIC <b>このペイン 1 つの中で、Step 3〜6 の項目をすべて設定します。</b>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC <b>「パイプライン設定」ペインの構成（UI 形式）</b>
# MAGIC
# MAGIC | セクション | 設定項目 | 設定方法 |
# MAGIC |---|---|---|
# MAGIC | パイプライン情報 | パイプラインID / タイプ / 名前 / モード | ペイン上部、鉛筆アイコンで直接編集 |
# MAGIC | **コードアセット** | ルートフォルダ + ソースコード | <b>「パスを構成」ボタン</b> → サブペイン |
# MAGIC | **データアセットのデフォルトの格納場所** | デフォルトカタログ + デフォルトスキーマ | <b>「カタログとスキーマを編集」ボタン</b> → サブペイン |
# MAGIC | **コンピュート** | サーバーレス / クラシッククラスター | 鉛筆アイコンで切替（今回はサーバーレスのまま） |
# MAGIC | パイプライン環境 | 依存ライブラリ | 今回は不要 |
# MAGIC | 設定（Custom） | Spark conf 等 | 今回は不要 |

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 3. コードアセット（ルートフォルダ + ソースコード）
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 3. コードアセット（ルートフォルダ + ソースコード）</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ ここが要注意ポイント</strong><br>
# MAGIC 「パイプライン設定」ペイン内の <b>「コードアセット」</b> セクションで <b>「パスを構成」ボタン</b>をクリックすると、<br>
# MAGIC <b>「パイプラインのソースコード」サブペイン</b>がさらに開きます。そこで <b>ルートフォルダ</b>と <b>ソースコード</b>を 2 段階で設定します。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC | 項目 | 値 | 補足 |
# MAGIC |---|---|---|
# MAGIC | **ルートフォルダ** | `/Workspace/Users/<your-email>/.../E2E_DataEngineerting/` | このフォルダが Python の `sys.path` に自動追加される |
# MAGIC | **ソースコード** | `/Workspace/Users/<your-email>/.../E2E_DataEngineerting/02_SDPパイプライン定義` | 02 のノートブックを直接指定 |
# MAGIC
# MAGIC ※「ソースコード」は <b>フォルダ</b>（中の SQL/Python ファイルすべてを取り込み）でも、<b>個別ファイル / ノートブック</b>でも指定可能。<br>
# MAGIC ※ <b>「+ パスを追加する」</b> ボタンで複数ソースを並べることもできます（今回は 1 行で OK）。
# MAGIC
# MAGIC 入力後、サブペイン下部の <b>「保存」ボタン</b>で「パイプライン設定」ペインに戻ります。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 4. データアセットのデフォルトの格納場所
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 4. データアセットのデフォルトの格納場所</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC 「パイプライン設定」ペイン内の <b>「データアセットのデフォルトの格納場所」</b> セクションで <b>「カタログとスキーマを編集」ボタン</b>をクリック。<br>
# MAGIC ➡ <b>「デフォルトの場所」サブペイン</b>が開きます。<br>
# MAGIC ここで、SDP がテーブルを作成するデフォルトのカタログ・スキーマを指定します。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,「デフォルトの場所」サブペインに入力する値
displayHTML(f"""
<table>
  <tr><th>項目</th><th>値</th></tr>
  <tr><td><b>デフォルトカタログ</b></td><td><code>{MY_CATALOG}</code></td></tr>
  <tr><td><b>デフォルトスキーマ</b></td><td><code>{MY_SCHEMA}</code></td></tr>
</table>
<p>※ 02 の <code>bz_*</code> / <code>sl_*</code> / <code>gd_*</code> テーブルはすべてこのスキーマ配下に作成されます。<br>
※ 設定後、サブペイン下部の <b>「保存」</b> で「パイプライン設定」ペインに戻ります。</p>
""")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 5. パイプライン名・モードを確認
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 5. パイプライン名・モードを確認</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 「パイプライン設定」ペインの上部、各項目の <b>鉛筆アイコン</b> から直接編集できます。
# MAGIC
# MAGIC | 項目 | 値 | 備考 |
# MAGIC |---|---|---|
# MAGIC | **パイプラインタイプ** | `ETLパイプライン` | デフォルト |
# MAGIC | **パイプライン名** | `used_car_e2e_pipeline` | 任意の名前 |
# MAGIC | **パイプラインモード** | `トリガー` | デフォルト。1 回実行して終了するモード |
# MAGIC
# MAGIC ※ 定期自動実行は別途、Step 7 の <b>「スケジュール」ボタン</b> から設定（Trigger type: `Scheduled` = cron 指定、`Continuous` = 連続実行）。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 6. パイプライン構成(Configuration)で `volume_path` を設定
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 6. パイプライン構成で <code>volume_path</code> を設定</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ 必須設定</strong><br>
# MAGIC <code>02_SDPパイプライン定義.sql</code> の <code>read_files()</code> パスは <code>${volume_path}/master/</code> の形でパラメータ化されています。<br>
# MAGIC 「パイプライン設定」ペインの <b>「構成(Configuration)」</b> セクションで以下のキーバリューを追加してください。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,「構成(Configuration)」に追加するキーバリュー
displayHTML(f"""
<table>
  <tr><th>キー</th><th>値</th></tr>
  <tr><td><code>volume_path</code></td><td><code>{VOLUME_PATH}</code></td></tr>
</table>
<p>※ この値は <code>00_config.py</code> の <code>VOLUME_PATH</code> と一致しています。<br>
※ JSON 表示モードに切り替えると、<code>"configuration": {{"volume_path": "{VOLUME_PATH}"}}</code> として表示されます。</p>
""")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 7. コンピュートを確認
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 7. コンピュートを確認</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC 「パイプライン設定」ペイン内の <b>「コンピュート」</b> セクションで <code>サーバーレス</code>(デフォルト)のままで OK。<br>
# MAGIC ※ サーバーレスでは Product edition の選択 UI は表示されず、Advanced 相当(Expectations 等すべて)が自動適用されます。<br>
# MAGIC 設定が終わったら <b>ペイン右上の「×」</b>で閉じます(保存はインライン)。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 8. (任意)スケジュールを設定
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 8. (任意)スケジュールを設定</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC 定期自動実行が必要な場合のみ、画面右上の <b>「スケジュール」ボタン</b> をクリック。<br>
# MAGIC ➡ <b>スケジュールダイアログ</b>が開きます。<br>
# MAGIC <b>Trigger type</b>：<code>Scheduled</code>（cron 指定）/ <code>Continuous</code>（連続実行）<br>
# MAGIC ※ デモでは手動実行で OK のため、このステップはスキップ可。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 9. （任意）ドライランで検証
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 9. （任意）ドライランで検証</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC 画面右上の <b>「ドライラン」ボタン</b> をクリックすると、<b>実データ処理を行わずに</b>パイプライン定義のみを検証できます。<br>
# MAGIC ✅ チェック内容：SQL/Python の構文エラー / テーブル間の依存関係 / カタログ・スキーマ・Volume パスの妥当性<br>
# MAGIC ⏱ 所要時間：<b>数十秒〜1 分程度</b>（コンピュート起動と検証のみ）<br>
# MAGIC ➡ 本番実行前の <b>設定ミスの早期発見</b> に有効。検証 OK なら次の Step 10 へ。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 10. 「パイプラインを実行」で起動
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 10. 「パイプラインを実行」で起動</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC パイプライン詳細画面の右上 <b>「パイプラインを実行」ボタン</b>をクリック。<br>
# MAGIC <b>初回実行は 5〜8 分</b> 程度（Serverless ウォームアップ含む）。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 11. 実行結果を確認(デモのハイライト)
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 11. 実行結果を確認(デモのハイライト)</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ 11-1. DAG（依存関係グラフ）</strong><br>
# MAGIC パイプライン詳細画面の中央に <b>15 ノードの DAG</b> が自動描画されます。<br>
# MAGIC <code>bz_*</code> → <code>sl_*</code> → <code>gd_*</code> の流れがクリック可能で、各ノードでスキーマ・行数・処理時間が確認できます。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ 11-2. Expectations の違反件数</strong><br>
# MAGIC 各 Silver テーブルのノードをクリック → <b>「Data quality」タブ</b> に切り替え。<br>
# MAGIC <code>vin_length_17</code> / <code>model_year_in_range</code> / <code>margin_consistency</code> 等のルールごとに<br>
# MAGIC <b>Passing records / Failing records / Pass rate</b> が自動集計されています。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,11-3. Lineage(データ系統)
displayHTML(f"""
<div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
<strong>✅ 11-3. Lineage(データ系統)</strong><br>
Catalog Explorer から <code>{MY_CATALOG}.{MY_SCHEMA}.gd_store_monthly_sales</code> を開き、<br>
<b>「Lineage」タブ</b> をクリック → CSV ファイル → <code>bz_*</code> → <code>sl_*</code> → <code>gd_*</code> の系統が<br>
<b>クリック可能なグラフ</b>で表示されます。
</div>
""")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ 11-4. Auto Loader の増分取り込み(任意デモ)</strong><br>
# MAGIC <code>01_データ準備.py</code> を再実行 → 直近 5 日分の日次 CSV が再配置される。<br>
# MAGIC その後パイプラインを再実行すると、<b>新規ファイルだけが取り込まれる</b>挙動が UI のメトリクスで確認できます。<br>
# MAGIC （bz_inquiries / bz_contracts のノードで「Records added」が新規ファイル分のみ増える）
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ パイプライン構築完了</strong><br>
# MAGIC Bronze 6 + Silver 6 + Gold 3 = <b>15 オブジェクト</b> が SDP として登録・実行されました。<br>
# MAGIC 次は <code>04_NB補完処理.py</code> で SDP では書きにくい複雑処理 + AI Functions による顧客プロファイル enrich に進みます。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ トラブルシューティング</strong><br>
# MAGIC <b>「No tables found」</b>：02 のソースパスが間違っている → 「設定」ペイン → コードアセットの「パスを構成」を再確認<br>
# MAGIC <b>「Path does not exist」</b>：01 が未実行 → Volume に CSV が無い → 01 を先に実行<br>
# MAGIC <b>テーブルが想定外のカタログ・スキーマに作られる</b>：「設定」ペイン → データアセットのデフォルトの格納場所 →「カタログとスキーマを編集」を再確認<br>
# MAGIC <b>Expectations の Pass rate が極端に低い</b>：CSV のフォーマットがずれている可能性 → bz_ レイヤを直接 SELECT してデータ確認<br>
# MAGIC <b>パイプラインが INITIALIZING で止まる</b>：Serverless のウォームアップ。3-5 分待つ<br>
# MAGIC <b>ソースコード設定で「フォルダが見つからない」</b>：パスが <code>/Workspace/Users/...</code> から始まっているか確認
# MAGIC </div>
