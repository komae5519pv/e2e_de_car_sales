# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 09 | Jobs ワークフロー作成手順
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">SDP パイプライン + ノートブックを 1 つの DAG で自動化する</p>
# MAGIC     </div>
# MAGIC     <div style="margin-left: auto;">
# MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 15 min</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #FFC107; background: #FFF8E1; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🎯 このノートブックのゴール</strong><br>
# MAGIC これまで手動で実行してきた処理(<code>01_データ準備</code> → <code>02 SDPパイプライン</code> → <code>04_AIによる非構造データ処理</code> → <code>05_テーブル設定</code> → <code>06_UC_Metrics_Views</code>)を、<br>
# MAGIC Databricks Jobs で <b>ワークフロー化(DAG)</b>し、自動実行できるようにします。<br>
# MAGIC <b>Pipeline タスク + Notebook タスクの混在 DAG</b> として組む点がポイントです。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📋 前提条件</strong><br>
# MAGIC ✅ <code>03_SDPパイプライン設定手順.py</code> で SDP パイプラインを作成済み(<b>pipeline_id を控えておく</b>)<br>
# MAGIC ✅ <code>04_AIによる非構造データ処理.py</code> / <code>05_テーブル設定.py</code> / <code>06_UC_Metrics_Views.py</code> が単体で実行できる状態<br>
# MAGIC ✅ <code>komae_whisper_large_v3</code> Model Serving エンドポイントが起動済み(04 で使用)<br>
# MAGIC ✅ サーバーレス SQL ウェアハウスが起動可能
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,共通設定の読み込み
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 1. Databricks Jobs とは
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">1. Databricks Jobs とは</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 Databricks Jobs とは</strong><br>
# MAGIC ノートブック・SDP パイプライン・SQL・dbt 等を <b>スケジュール実行</b>・<b>依存関係付き DAG 実行</b>できる機能です。
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li><b>タスク DAG</b>: タスク間の依存関係を定義し、並列 / 直列に実行</li>
# MAGIC   <li><b>多様なタスクタイプ</b>: Notebook / Pipeline(SDP) / SQL / Python wheel / dbt / If/else 等</li>
# MAGIC   <li><b>スケジュール</b>: cron 形式で定期実行(毎朝 6 時、毎時間 等)</li>
# MAGIC   <li><b>リトライ / アラート</b>: 失敗時の自動リトライ、メール / Slack 通知</li>
# MAGIC   <li><b>Serverless</b>: コンピュートの起動待ち時間ゼロで実行</li>
# MAGIC   <li><b>Repair run</b>: 失敗したタスクだけを再実行(最初からやり直す必要なし)</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 2. 作成する DAG 構成
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">2. 作成する DAG 構成</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 パイプラインの全体像</strong><br>
# MAGIC 5 タスクを順番に実行する直列 DAG を作成します。<br>
# MAGIC ポイントは <b>2 番目のタスクが Notebook ではなく Pipeline タスクタイプ</b>であること(SDP パイプラインを呼び出す)。
# MAGIC </div>
# MAGIC <div style="display: flex; justify-content: center; margin: 16px 0;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1080 120" width="1020">
# MAGIC   <defs>
# MAGIC     <marker id="arr" viewBox="0 0 10 7" refX="10" refY="3.5" markerWidth="10" markerHeight="7" orient="auto">
# MAGIC       <polygon points="0 0, 10 3.5, 0 7" fill="#94a3b8"/>
# MAGIC     </marker>
# MAGIC   </defs>
# MAGIC   <style>text{font-family:"Helvetica Neue",Helvetica,Arial,"Hiragino Kaku Gothic ProN","Hiragino Sans",Meiryo,sans-serif}</style>
# MAGIC   <rect x="0" y="10" width="200" height="56" rx="6" fill="#ffffff" stroke="#e2e8f0" stroke-width="1.5"/>
# MAGIC   <rect x="0" y="10" width="4" height="56" rx="2" fill="#8b5cf6"/>
# MAGIC   <text x="16" y="33" font-size="12" font-weight="600" fill="#1e293b">init_data (Notebook)</text>
# MAGIC   <text x="16" y="52" font-size="10" fill="#94a3b8">01 — CSV/PDF/音声をVolume配置</text>
# MAGIC   <line x1="200" y1="38" x2="230" y2="38" stroke="#94a3b8" stroke-width="1.5" marker-end="url(#arr)"/>
# MAGIC   <rect x="235" y="10" width="200" height="56" rx="6" fill="#ffffff" stroke="#e2e8f0" stroke-width="1.5"/>
# MAGIC   <rect x="235" y="10" width="4" height="56" rx="2" fill="#3b82f6"/>
# MAGIC   <text x="251" y="33" font-size="12" font-weight="600" fill="#1e293b">sdp_pipeline (Pipeline)</text>
# MAGIC   <text x="251" y="52" font-size="10" fill="#94a3b8">02 SDP — bz/sl/gd 自動構築</text>
# MAGIC   <line x1="435" y1="38" x2="465" y2="38" stroke="#94a3b8" stroke-width="1.5" marker-end="url(#arr)"/>
# MAGIC   <rect x="470" y="10" width="200" height="56" rx="6" fill="#ffffff" stroke="#e2e8f0" stroke-width="1.5"/>
# MAGIC   <rect x="470" y="10" width="4" height="56" rx="2" fill="#f59e0b"/>
# MAGIC   <text x="486" y="33" font-size="12" font-weight="600" fill="#1e293b">ai_enrich (Notebook)</text>
# MAGIC   <text x="486" y="52" font-size="10" fill="#94a3b8">04 — 音声→Whisper / AI Functions</text>
# MAGIC   <line x1="670" y1="38" x2="700" y2="38" stroke="#94a3b8" stroke-width="1.5" marker-end="url(#arr)"/>
# MAGIC   <rect x="705" y="10" width="180" height="56" rx="6" fill="#ffffff" stroke="#e2e8f0" stroke-width="1.5"/>
# MAGIC   <rect x="705" y="10" width="4" height="56" rx="2" fill="#22c55e"/>
# MAGIC   <text x="721" y="33" font-size="12" font-weight="600" fill="#1e293b">governance (NB)</text>
# MAGIC   <text x="721" y="52" font-size="10" fill="#94a3b8">05 — PK/FK・コメント・PII</text>
# MAGIC   <line x1="885" y1="38" x2="915" y2="38" stroke="#94a3b8" stroke-width="1.5" marker-end="url(#arr)"/>
# MAGIC   <rect x="920" y="10" width="160" height="56" rx="6" fill="#ffffff" stroke="#e2e8f0" stroke-width="1.5"/>
# MAGIC   <rect x="920" y="10" width="4" height="56" rx="2" fill="#0ea5e9"/>
# MAGIC   <text x="936" y="33" font-size="12" font-weight="600" fill="#1e293b">metric_views (NB)</text>
# MAGIC   <text x="936" y="52" font-size="10" fill="#94a3b8">06 — UC Metric Views</text>
# MAGIC   <rect x="0" y="84" width="7" height="7" rx="2" fill="#8b5cf6"/>
# MAGIC   <text x="11" y="91" font-size="9" fill="#94a3b8">Init</text>
# MAGIC   <rect x="42" y="84" width="7" height="7" rx="2" fill="#3b82f6"/>
# MAGIC   <text x="53" y="91" font-size="9" fill="#94a3b8">SDP Pipeline</text>
# MAGIC   <rect x="125" y="84" width="7" height="7" rx="2" fill="#f59e0b"/>
# MAGIC   <text x="136" y="91" font-size="9" fill="#94a3b8">AI Enrich</text>
# MAGIC   <rect x="195" y="84" width="7" height="7" rx="2" fill="#22c55e"/>
# MAGIC   <text x="206" y="91" font-size="9" fill="#94a3b8">Governance</text>
# MAGIC   <rect x="275" y="84" width="7" height="7" rx="2" fill="#0ea5e9"/>
# MAGIC   <text x="286" y="91" font-size="9" fill="#94a3b8">Metric Views</text>
# MAGIC </svg>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 3. ジョブの作成手順
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">3. ジョブの作成手順</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: Step 1 — ジョブを新規作成</strong><br>
# MAGIC <ol style="margin: 6px 0 0 0;">
# MAGIC   <li>左メニューの「<b>Jobs &amp; Pipelines</b>」をクリック</li>
# MAGIC   <li>右上「<b>Create</b>」 → 「<b>Job</b>」をクリック</li>
# MAGIC   <li>ジョブ名を入力: <b><code>used_car_e2e_job</code></b></li>
# MAGIC </ol>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: Step 2 — タスクを順番に追加</strong><br><br>
# MAGIC 以下の 5 タスクを順番に追加してください。<br>
# MAGIC ノートブックは <b>このノートブックと同じフォルダ</b>から選択します。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #7B1FA2; background: #F3E5F5; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔧 タスク構成</strong>
# MAGIC <table style="width: 100%; border-collapse: collapse; border-radius: 8px; overflow: hidden; margin-top: 8px;">
# MAGIC   <thead>
# MAGIC     <tr style="background: #7B1FA2; color: white;">
# MAGIC       <th style="padding: 10px 15px; text-align: left;">タスク名</th>
# MAGIC       <th style="padding: 10px 15px; text-align: left;">タイプ</th>
# MAGIC       <th style="padding: 10px 15px; text-align: left;">対象</th>
# MAGIC       <th style="padding: 10px 15px; text-align: left;">依存先</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background: #f9f9f9;">
# MAGIC       <td style="padding: 8px 15px;"><b>init_data</b></td>
# MAGIC       <td style="padding: 8px 15px;">Notebook</td>
# MAGIC       <td style="padding: 8px 15px;"><code>01_データ準備</code>(CSV / PDF / 音声を Volume に配置)</td>
# MAGIC       <td style="padding: 8px 15px;">(なし — 最初に実行)</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 8px 15px;"><b>sdp_pipeline</b></td>
# MAGIC       <td style="padding: 8px 15px;"><b>Pipeline</b></td>
# MAGIC       <td style="padding: 8px 15px;">03 で作成した <code>used_car_e2e_pipeline</code></td>
# MAGIC       <td style="padding: 8px 15px;">init_data</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #f9f9f9;">
# MAGIC       <td style="padding: 8px 15px;"><b>ai_enrich</b></td>
# MAGIC       <td style="padding: 8px 15px;">Notebook</td>
# MAGIC       <td style="padding: 8px 15px;"><code>04_AIによる非構造データ処理</code></td>
# MAGIC       <td style="padding: 8px 15px;">sdp_pipeline</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 8px 15px;"><b>governance</b></td>
# MAGIC       <td style="padding: 8px 15px;">Notebook</td>
# MAGIC       <td style="padding: 8px 15px;"><code>05_テーブル設定</code></td>
# MAGIC       <td style="padding: 8px 15px;">ai_enrich</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #f9f9f9;">
# MAGIC       <td style="padding: 8px 15px;"><b>metric_views</b></td>
# MAGIC       <td style="padding: 8px 15px;">Notebook</td>
# MAGIC       <td style="padding: 8px 15px;"><code>06_UC_Metrics_Views</code></td>
# MAGIC       <td style="padding: 8px 15px;">governance</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: Step 3 — 各タスクの設定詳細</strong><br><br>
# MAGIC <b>① Notebook タスク(init_data / ai_enrich / governance / metric_views)</b>
# MAGIC <ol style="margin: 6px 0 0 0;">
# MAGIC   <li><b>Type</b>: <code>Notebook</code> を選択</li>
# MAGIC   <li><b>Source</b>: <code>Workspace</code> を選択</li>
# MAGIC   <li><b>Path</b>: 該当ノートブックを指定(このノートブックと同じフォルダ内)</li>
# MAGIC   <li><b>Compute</b>: <code>Serverless</code> を選択(起動待ち時間ゼロ)</li>
# MAGIC   <li><b>Depends on</b>: 上の表の依存先タスクを選択</li>
# MAGIC </ol>
# MAGIC <br>
# MAGIC <b>② Pipeline タスク(sdp_pipeline)</b>
# MAGIC <ol style="margin: 6px 0 0 0;">
# MAGIC   <li><b>Type</b>: <b><code>Pipeline</code></b> を選択(Notebook ではない!)</li>
# MAGIC   <li><b>Pipeline</b>: 03 で作成した <code>used_car_e2e_pipeline</code> をプルダウンから選択</li>
# MAGIC   <li><b>Full refresh</b>: 通常はチェックなし(差分実行)。全件再構築したいときのみ ON</li>
# MAGIC   <li><b>Depends on</b>: <code>init_data</code></li>
# MAGIC </ol>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 Pipeline タスクのメリット</strong><br>
# MAGIC SDP パイプラインを Notebook タスクで実行することもできますが、<b>Pipeline タスクタイプ</b>を使うと:
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li>パイプラインの DAG・Expectations・Lineage が <b>Jobs の UI から直接確認</b>できる</li>
# MAGIC   <li>失敗時は <b>パイプライン側のリトライ機構</b>が活用される</li>
# MAGIC   <li>パイプラインの設定(構成パラメータ・スケジュール等)は SDP 側で一元管理される</li>
# MAGIC </ul>
# MAGIC ➡ SDP を本番運用するときは <b>Pipeline タスクタイプ</b>が定石です。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 4. ジョブの実行
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">4. ジョブの実行</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: Step 4 — ジョブを実行する</strong><br>
# MAGIC <ol style="margin: 6px 0 0 0;">
# MAGIC   <li>右上の「<b>Run now</b>」をクリック</li>
# MAGIC   <li>DAG ビューで各タスクの実行状況を確認</li>
# MAGIC   <li>全タスクが緑色(成功)になれば完了</li>
# MAGIC </ol>
# MAGIC <br>
# MAGIC 💡 タスクをクリックすると、そのノートブック / パイプラインの実行結果を確認できます。<br>
# MAGIC 💡 <b>初回フル実行は 15〜20 分</b>(SDP パイプラインの初回構築 + AI Functions 呼び出しを含むため)。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ 失敗した場合</strong><br>
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li>失敗したタスク(赤色)をクリックしてエラーログを確認</li>
# MAGIC   <li>ノートブックのパス / パイプラインの選択 / コンピュートの設定が正しいか確認</li>
# MAGIC   <li>修正後、失敗したタスクだけを「<b>Repair run</b>」で再実行(最初からやり直す必要なし)</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 5. スケジュール設定(オプション)
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">5. スケジュール設定(オプション)</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: スケジュールを設定する</strong><br><br>
# MAGIC 本番運用では、このジョブを定期実行するようスケジュール設定します。<br>
# MAGIC 中古車販売は <b>朝に在庫・引合データが更新</b>されるため、早朝実行が定石。
# MAGIC <ol style="margin: 6px 0 0 0;">
# MAGIC   <li>ジョブ画面の右上「<b>Add trigger</b>」 → 「<b>Scheduled</b>」をクリック</li>
# MAGIC   <li>スケジュールを設定(例: <b>毎日 6:00 AM JST</b>)</li>
# MAGIC   <li>タイムゾーンが <b>Asia/Tokyo</b> になっていることを確認</li>
# MAGIC   <li>「<b>Save</b>」をクリック</li>
# MAGIC </ol>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 本番運用で追加したい設定</strong>
# MAGIC <table style="width: 100%; border-collapse: collapse; border-radius: 8px; overflow: hidden; margin-top: 8px;">
# MAGIC   <thead>
# MAGIC     <tr style="background: #1976D2; color: white;">
# MAGIC       <th style="padding: 10px 15px; text-align: left;">設定項目</th>
# MAGIC       <th style="padding: 10px 15px; text-align: left;">推奨値</th>
# MAGIC       <th style="padding: 10px 15px; text-align: left;">設定場所</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background: #f9f9f9;">
# MAGIC       <td style="padding: 8px 15px;">リトライ回数</td>
# MAGIC       <td style="padding: 8px 15px;">2 回</td>
# MAGIC       <td style="padding: 8px 15px;">各タスク → Advanced → Retries</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 8px 15px;">タイムアウト</td>
# MAGIC       <td style="padding: 8px 15px;">60 分(SDP + AI Functions のため余裕を持たせる)</td>
# MAGIC       <td style="padding: 8px 15px;">各タスク → Advanced → Timeout</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #f9f9f9;">
# MAGIC       <td style="padding: 8px 15px;">メール通知</td>
# MAGIC       <td style="padding: 8px 15px;">失敗時に管理者へ通知</td>
# MAGIC       <td style="padding: 8px 15px;">ジョブ設定 → Notifications</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 8px 15px;">同時実行数</td>
# MAGIC       <td style="padding: 8px 15px;">1(同じジョブの多重起動を防ぐ)</td>
# MAGIC       <td style="padding: 8px 15px;">ジョブ設定 → Maximum concurrent runs</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 6. 初回限定処理を条件分岐で制御(If/else パターン)
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">6. 初回限定処理を条件分岐で制御(If/else パターン)</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 なぜ governance を毎回実行する必要がないのか</strong><br>
# MAGIC <code>governance</code>(05_テーブル設定)で行う処理は <b>テーブルのコメント・PK/FK 制約・PII マスキング</b>の設定です。<br>
# MAGIC これらは <b>テーブル定義(メタデータ)</b>に対する操作で、一度設定すれば残り続けます。
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li>毎日のスケジュール実行で <b>毎回再設定するのは無駄</b>(処理時間・ウェアハウス利用料の浪費)</li>
# MAGIC   <li>SDP がスキーマを変更した場合のみ <b>再実行が必要</b>(カラム追加 / テーブル新設 等)</li>
# MAGIC   <li>つまり <b>初回 + 必要なときだけ</b>実行できれば十分</li>
# MAGIC </ul>
# MAGIC ➡ <b>Job Parameter + If/else condition タスク</b>で制御するのが定石です。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 完成形の DAG</strong><br>
# MAGIC <code>ai_enrich</code> のあとに <b>If/else condition タスク</b>を挟み、パラメータ <code>run_governance</code> の値で分岐します。<br>
# MAGIC <span style="color: #16a34a;"><b>緑の経路(true)</b></span>では governance を実行、<span style="color: #dc2626;"><b>赤の経路(false)</b></span>ではスキップして直接 metric_views へ進みます。
# MAGIC </div>
# MAGIC <div style="display: flex; justify-content: center; margin: 16px 0;">
# MAGIC <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1100 200" width="1080">
# MAGIC   <defs>
# MAGIC     <marker id="arr2" viewBox="0 0 10 7" refX="10" refY="3.5" markerWidth="10" markerHeight="7" orient="auto">
# MAGIC       <polygon points="0 0, 10 3.5, 0 7" fill="#94a3b8"/>
# MAGIC     </marker>
# MAGIC     <marker id="arrG" viewBox="0 0 10 7" refX="10" refY="3.5" markerWidth="10" markerHeight="7" orient="auto">
# MAGIC       <polygon points="0 0, 10 3.5, 0 7" fill="#22c55e"/>
# MAGIC     </marker>
# MAGIC     <marker id="arrR" viewBox="0 0 10 7" refX="10" refY="3.5" markerWidth="10" markerHeight="7" orient="auto">
# MAGIC       <polygon points="0 0, 10 3.5, 0 7" fill="#ef4444"/>
# MAGIC     </marker>
# MAGIC   </defs>
# MAGIC   <style>text{font-family:"Helvetica Neue",Helvetica,Arial,"Hiragino Kaku Gothic ProN","Hiragino Sans",Meiryo,sans-serif}</style>
# MAGIC   <!-- init_data -->
# MAGIC   <rect x="10" y="80" width="152" height="56" rx="6" fill="#ffffff" stroke="#e2e8f0" stroke-width="1.5"/>
# MAGIC   <rect x="10" y="80" width="4" height="56" rx="2" fill="#8b5cf6"/>
# MAGIC   <text x="22" y="103" font-size="12" font-weight="600" fill="#1e293b">init_data</text>
# MAGIC   <text x="22" y="122" font-size="10" fill="#94a3b8">Notebook</text>
# MAGIC   <line x1="162" y1="108" x2="190" y2="108" stroke="#94a3b8" stroke-width="1.5" marker-end="url(#arr2)"/>
# MAGIC   <!-- sdp_pipeline -->
# MAGIC   <rect x="195" y="80" width="152" height="56" rx="6" fill="#ffffff" stroke="#e2e8f0" stroke-width="1.5"/>
# MAGIC   <rect x="195" y="80" width="4" height="56" rx="2" fill="#3b82f6"/>
# MAGIC   <text x="207" y="103" font-size="12" font-weight="600" fill="#1e293b">sdp_pipeline</text>
# MAGIC   <text x="207" y="122" font-size="10" fill="#94a3b8">Pipeline</text>
# MAGIC   <line x1="347" y1="108" x2="375" y2="108" stroke="#94a3b8" stroke-width="1.5" marker-end="url(#arr2)"/>
# MAGIC   <!-- ai_enrich -->
# MAGIC   <rect x="380" y="80" width="152" height="56" rx="6" fill="#ffffff" stroke="#e2e8f0" stroke-width="1.5"/>
# MAGIC   <rect x="380" y="80" width="4" height="56" rx="2" fill="#f59e0b"/>
# MAGIC   <text x="392" y="103" font-size="12" font-weight="600" fill="#1e293b">ai_enrich</text>
# MAGIC   <text x="392" y="122" font-size="10" fill="#94a3b8">Notebook</text>
# MAGIC   <line x1="532" y1="108" x2="555" y2="108" stroke="#94a3b8" stroke-width="1.5"/>
# MAGIC   <!-- if_governance diamond -->
# MAGIC   <polygon points="555,108 622,72 689,108 622,144" fill="#fef9c3" stroke="#ca8a04" stroke-width="1.8"/>
# MAGIC   <text x="622" y="103" font-size="11" font-weight="700" fill="#713f12" text-anchor="middle">if_governance</text>
# MAGIC   <text x="622" y="118" font-size="9" fill="#854d0e" text-anchor="middle">run_governance == true ?</text>
# MAGIC   <!-- True path (green): diamond top -> governance -->
# MAGIC   <path d="M 622 72 Q 622 40 690 30" fill="none" stroke="#22c55e" stroke-width="2" marker-end="url(#arrG)"/>
# MAGIC   <text x="635" y="55" font-size="11" font-weight="700" fill="#16a34a">true</text>
# MAGIC   <!-- governance (top row, green) -->
# MAGIC   <rect x="700" y="10" width="152" height="44" rx="6" fill="#ffffff" stroke="#22c55e" stroke-width="1.8"/>
# MAGIC   <rect x="700" y="10" width="4" height="44" rx="2" fill="#22c55e"/>
# MAGIC   <text x="712" y="29" font-size="12" font-weight="600" fill="#1e293b">governance</text>
# MAGIC   <text x="712" y="44" font-size="10" fill="#16a34a">初回 / スキーマ変更時のみ</text>
# MAGIC   <!-- governance -> metric_views (curve back down) -->
# MAGIC   <path d="M 852 32 Q 920 32 940 80" fill="none" stroke="#94a3b8" stroke-width="1.5" marker-end="url(#arr2)"/>
# MAGIC   <!-- False path (red dashed): diamond right -> metric_views -->
# MAGIC   <path d="M 689 108 L 940 108" fill="none" stroke="#ef4444" stroke-width="2" stroke-dasharray="6 4" marker-end="url(#arrR)"/>
# MAGIC   <text x="780" y="100" font-size="11" font-weight="700" fill="#dc2626">false (skip)</text>
# MAGIC   <!-- metric_views -->
# MAGIC   <rect x="945" y="80" width="145" height="56" rx="6" fill="#ffffff" stroke="#e2e8f0" stroke-width="1.5"/>
# MAGIC   <rect x="945" y="80" width="4" height="56" rx="2" fill="#0ea5e9"/>
# MAGIC   <text x="957" y="103" font-size="12" font-weight="600" fill="#1e293b">metric_views</text>
# MAGIC   <text x="957" y="122" font-size="10" fill="#94a3b8">Run if: All done</text>
# MAGIC   <!-- Legend -->
# MAGIC   <line x1="10" y1="175" x2="40" y2="175" stroke="#22c55e" stroke-width="2"/>
# MAGIC   <text x="46" y="179" font-size="10" fill="#475569">true 経路: governance を実行</text>
# MAGIC   <line x1="240" y1="175" x2="270" y2="175" stroke="#ef4444" stroke-width="2" stroke-dasharray="6 4"/>
# MAGIC   <text x="276" y="179" font-size="10" fill="#475569">false 経路: governance をスキップ</text>
# MAGIC   <text x="510" y="179" font-size="10" fill="#475569">※ metric_views は <tspan font-weight="700">Run if = All done</tspan> でどちらの経路からも合流</text>
# MAGIC </svg>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: Step 1 — ジョブパラメータを定義</strong><br>
# MAGIC <ol style="margin: 6px 0 0 0;">
# MAGIC   <li>ジョブの編集画面右側のサイドバーで「<b>Parameters</b>」を開く</li>
# MAGIC   <li>「<b>パラメーターを編集</b>」をクリック</li>
# MAGIC   <li>以下の値を入力:
# MAGIC     <ul>
# MAGIC       <li><b>Key</b>: <code>run_governance</code></li>
# MAGIC       <li><b>Default value</b>: <code>false</code></li>
# MAGIC     </ul>
# MAGIC   </li>
# MAGIC   <li>「<b>Save</b>」をクリック</li>
# MAGIC </ol>
# MAGIC <br>
# MAGIC 💡 デフォルトを <code>false</code> にすることで、<b>通常のスケジュール実行では governance がスキップ</b>されます。<br>
# MAGIC 必要なときだけ「Run now with different parameters」で <code>true</code> を指定して再設定できます。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: Step 2 — If/else condition タスクを追加</strong><br>
# MAGIC <ol style="margin: 6px 0 0 0;">
# MAGIC   <li>DAG ビューの「<b>+ Add task</b>」をクリック</li>
# MAGIC   <li>以下を設定:
# MAGIC     <ul>
# MAGIC       <li><b>Task name</b>: <code>if_governance</code></li>
# MAGIC       <li><b>Type</b>: <b><code>If/else condition</code></b></li>
# MAGIC       <li><b>Condition</b>: <code>{{job.parameters.run_governance}}</code> <code>==</code> <code>true</code></li>
# MAGIC       <li><b>Depends on</b>: <code>ai_enrich</code></li>
# MAGIC     </ul>
# MAGIC   </li>
# MAGIC   <li>「<b>Save task</b>」をクリック</li>
# MAGIC </ol>
# MAGIC <br>
# MAGIC 💡 condition は <b>文字列比較</b>です。左辺に <code>{{job.parameters.run_governance}}</code>、右辺に <code>true</code> を入れます。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: Step 3 — 依存関係を組み替える</strong><br><br>
# MAGIC <b>① governance タスク</b>(既存)
# MAGIC <ol style="margin: 6px 0 0 0;">
# MAGIC   <li>governance タスクを開く</li>
# MAGIC   <li><b>Depends on</b>: <code>ai_enrich</code> を削除し、<code>if_governance (true)</code> に変更</li>
# MAGIC </ol>
# MAGIC <br>
# MAGIC <b>② metric_views タスク</b>(既存)
# MAGIC <ol style="margin: 6px 0 0 0;">
# MAGIC   <li>metric_views タスクを開く</li>
# MAGIC   <li><b>Depends on</b>: <code>governance</code> はそのまま残し、<code>if_governance (false)</code> を追加(2 つの依存先になる)</li>
# MAGIC   <li><b>Advanced</b> → <b>Run if dependencies</b> を <b><code>All done</code></b> に変更</li>
# MAGIC </ol>
# MAGIC <br>
# MAGIC 💡 <b>合流の仕組み</b>: true 経路では <code>governance</code> 経由で合流、false 経路では <code>if_governance (false)</code> から直接合流。<br>
# MAGIC ⚠️ <b>Run if = All done</b> がポイント。デフォルトの <code>All succeeded</code> では <b>スキップ(Excluded)になった依存先があると metric_views も実行されません</b>。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 Run if dependencies の選択肢</strong>
# MAGIC <table style="width: 100%; border-collapse: collapse; border-radius: 8px; overflow: hidden; margin-top: 8px;">
# MAGIC   <thead>
# MAGIC     <tr style="background: #1976D2; color: white;">
# MAGIC       <th style="padding: 10px 15px; text-align: left;">設定値</th>
# MAGIC       <th style="padding: 10px 15px; text-align: left;">挙動</th>
# MAGIC       <th style="padding: 10px 15px; text-align: left;">If/else との相性</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background: #f9f9f9;">
# MAGIC       <td style="padding: 8px 15px;"><b>All succeeded</b>(デフォルト)</td>
# MAGIC       <td style="padding: 8px 15px;">全ての依存タスクが成功時のみ実行(<b>スキップは成功扱いではない</b>)</td>
# MAGIC       <td style="padding: 8px 15px;">❌ 分岐先がスキップされると後続も止まる</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 8px 15px;"><b>All done</b></td>
# MAGIC       <td style="padding: 8px 15px;">依存タスクの結果(成功 / 失敗 / スキップ)に関わらず実行</td>
# MAGIC       <td style="padding: 8px 15px;">✅ 分岐後の合流点に最適</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #f9f9f9;">
# MAGIC       <td style="padding: 8px 15px;"><b>At least one succeeded</b></td>
# MAGIC       <td style="padding: 8px 15px;">少なくとも 1 つの依存が成功すれば実行</td>
# MAGIC       <td style="padding: 8px 15px;">⚪ 並列分岐のいずれかが成功すれば良い場合</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 8px 15px;"><b>None failed</b></td>
# MAGIC       <td style="padding: 8px 15px;">失敗が無ければ実行(スキップは OK)</td>
# MAGIC       <td style="padding: 8px 15px;">⚪ 失敗だけ防ぎたい場合</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: Step 4 — 動作確認</strong><br><br>
# MAGIC <b>① 通常実行(governance スキップ)</b>
# MAGIC <ol style="margin: 6px 0 0 0;">
# MAGIC   <li>「<b>Run now</b>」をクリック(パラメータはデフォルトの <code>false</code> が使われる)</li>
# MAGIC   <li>DAG ビューで <code>governance</code> が <b>「Excluded」</b>(灰色)になることを確認</li>
# MAGIC   <li><code>metric_views</code> はスキップを跨いで実行されることを確認(All done のおかげ)</li>
# MAGIC </ol>
# MAGIC <br>
# MAGIC <b>② パラメータを上書きして実行(governance も実行)</b>
# MAGIC <ol style="margin: 6px 0 0 0;">
# MAGIC   <li>右上の「<b>Run now</b>」横の <b>▼</b> → 「<b>Run now with different parameters</b>」をクリック</li>
# MAGIC   <li><code>run_governance</code> の値を <code>true</code> に書き換えて実行</li>
# MAGIC   <li><code>governance</code> が緑の経路で実行されることを確認</li>
# MAGIC </ol>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 応用例 — If/else condition の使いどころ</strong>
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li><b>月初のみ実行</b>: パラメータに日付を渡し、<code>{{job.parameters.run_date}}</code> を 1 日と比較</li>
# MAGIC   <li><b>ad-hoc 再処理</b>: <code>full_refresh</code> パラメータで SDP の Full refresh を切り替え</li>
# MAGIC   <li><b>環境別の切り替え</b>: <code>env</code> パラメータで dev / prod のテーブル先を分岐</li>
# MAGIC   <li><b>失敗時のリカバリ専用ジョブ起動</b>: 失敗フラグで通知タスクや再処理タスクを起動</li>
# MAGIC </ul>
# MAGIC ➡ ジョブを <b>1 つに集約しつつ柔軟に挙動を切り替え</b>られるため、運用が大幅にシンプルになります。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 7. クリーンアップ(オプション)
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">7. クリーンアップ(オプション)</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #D32F2F; background: #FFEBEE; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🗑️ ハンズオン環境のクリーンアップ</strong><br>
# MAGIC ハンズオン終了後、作成したリソースを削除する場合:
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li><b>ジョブの削除</b>: Jobs &amp; Pipelines → 該当ジョブ → 右上「⋮」 → Delete</li>
# MAGIC   <li><b>SDP パイプラインの削除</b>: Jobs &amp; Pipelines → 該当パイプライン → 右上「⋮」 → Delete</li>
# MAGIC   <li><b>Genie / ダッシュボードの削除</b>: 各画面の「⋮」メニューから削除</li>
# MAGIC   <li><b>スキーマの削除</b>: 以下の SQL を実行(<b>データ・テーブル・ビューが全て削除されます</b>)</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,クリーンアップ(必要な場合のみコメント解除して実行)
# 以下のコメントを外して実行するとスキーマごと削除されます
# spark.sql(f"DROP SCHEMA IF EXISTS {MY_CATALOG}.{MY_SCHEMA} CASCADE")
# print(f"🗑️ スキーマ '{MY_CATALOG}.{MY_SCHEMA}' を削除しました")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ E2E ハンズオン完了!</strong><br>
# MAGIC おつかれさまでした!以下を体験しました:
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li><b>00</b>: Unity Catalog 環境設定(カタログ &gt; スキーマ &gt; Volume)</li>
# MAGIC   <li><b>01</b>: サンプルデータ生成(CSV 8 ファイル + PDF カタログ 5 種 + 商談録音 MP3)</li>
# MAGIC   <li><b>02 / 03</b>: SDP(Lakeflow Spark Declarative Pipelines)で Bronze / Silver / Gold を宣言的に構築</li>
# MAGIC   <li><b>04</b>: AI による非構造データ処理(音声 → Whisper 文字起こし / <code>ai_classify</code> / <code>ai_extract</code> / <code>ai_query</code> / <code>ai_parse_document</code>)</li>
# MAGIC   <li><b>05</b>: テーブル設定(コメント・PK/FK・PII マスキング)</li>
# MAGIC   <li><b>06</b>: UC Metric Views(業務指標の一元管理)</li>
# MAGIC   <li><b>07</b>: Genie スペース(自然言語 BI / エージェントモード)</li>
# MAGIC   <li><b>08</b>: AI/BI ダッシュボード(Genie Code でノーコード作成)</li>
# MAGIC   <li><b>09</b>: Jobs ワークフロー(Pipeline + Notebook 混在 DAG の自動化)</li>
# MAGIC </ul>
# MAGIC <br>
# MAGIC これで、<b>データ取り込み → 加工 → AI enrich → ガバナンス → 指標化 → BI / 自然言語分析 → 自動化</b> までの<br>
# MAGIC E2E パイプラインを Databricks 上でフル体験できました!
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ トラブルシューティング</strong><br>
# MAGIC <b>Pipeline タスクで「No pipeline found」</b>: 03 のパイプライン作成が未完了 / 別ワークスペース。03 の手順で作成済みか確認<br>
# MAGIC <b>ai_enrich タスクが失敗(<code>ai_parse_document</code> エラー)</b>: コンピュートが Serverless / DBR 17.1+ になっているか確認<br>
# MAGIC <b>governance タスクで「テーブルが見つかりません」</b>: SDP パイプラインがまだ完了していない可能性 → DAG 上での依存関係を再確認<br>
# MAGIC <b>metric_views タスクが「DBR 17.2+ required」</b>: コンピュートを Serverless または DBR 17.2+ クラスタに変更<br>
# MAGIC <b>ジョブ全体が遅い</b>: 初回は SDP パイプラインのウォームアップ + AI Functions のコールドスタートのため 15〜20 分かかります(2 回目以降は高速)<br>
# MAGIC <b>Repair run が表示されない</b>: 「Run now」直後はまだ表示されません。少なくとも 1 タスクが失敗してから現れます
# MAGIC </div>
