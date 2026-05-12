# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 06 | UC Metric Views(統一 KPI 定義)
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">販売・在庫・商談 KPI を YAML で一元定義</p>
# MAGIC     </div>
# MAGIC     <div style="margin-left: auto;">
# MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 20 min</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #ffc107;background:#fffde7;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">🎯</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">このノートブックのゴール</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         中古車販売の KPI(販売・在庫・商談)を <strong>UC Metric Views</strong> として YAML で定義し、<br/>
# MAGIC         Genie / AI/BI ダッシュボード / SQL から <strong>同じ定義</strong>でクエリできる状態を作ります。<br/>
# MAGIC         <strong>原則: Catalog Explorer + Genie Code(日本語指示)で作成。SQL はオプション。</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC <strong>📋 前提条件</strong><br>
# MAGIC ✅ <code>03_SDPパイプライン設定手順.py</code> 実行済み(<code>sl_*</code> テーブル作成済み)<br>
# MAGIC ✅ <code>05_テーブル設定.py</code> 実行済み(任意・コメント / マスキング含む場合)<br>
# MAGIC ✅ <b>Databricks Runtime 17.2+</b> または <b>サーバーレス SQL</b>(YAML version 1.1 のため)
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Metric View とは?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin:16px 0;">
# MAGIC   <div style="border:1px solid #E0E0E0;border-radius:8px;padding:20px;border-top:3px solid #FF3621;text-align:center;">
# MAGIC     <div style="font-size:28px;margin-bottom:8px;">🗄️</div>
# MAGIC     <div style="font-weight:700;font-size:15px;margin-bottom:6px;">Source(ベーステーブル)</div>
# MAGIC     <div style="font-size:13px;color:#666;line-height:1.5;">集計対象のファクトテーブル。<br/><code style="background:#f5f5f5;padding:2px 6px;border-radius:3px;font-size:12px;">source: sl_contracts</code></div>
# MAGIC   </div>
# MAGIC   <div style="border:1px solid #E0E0E0;border-radius:8px;padding:20px;border-top:3px solid #7B61FF;text-align:center;">
# MAGIC     <div style="font-size:28px;margin-bottom:8px;">🔀</div>
# MAGIC     <div style="font-weight:700;font-size:15px;margin-bottom:6px;">Dimensions(切り口)</div>
# MAGIC     <div style="font-size:13px;color:#666;line-height:1.5;">GROUP BY の候補。<br/>店舗・月・車種カテゴリ・年式区分など</div>
# MAGIC   </div>
# MAGIC   <div style="border:1px solid #E0E0E0;border-radius:8px;padding:20px;border-top:3px solid #FF6F00;text-align:center;">
# MAGIC     <div style="font-size:28px;margin-bottom:8px;">📊</div>
# MAGIC     <div style="font-weight:700;font-size:15px;margin-bottom:6px;">Measures(指標)</div>
# MAGIC     <div style="font-size:13px;color:#666;line-height:1.5;">集計ロジック(SUM, AVG 等)を定義。<br/>クエリ時に <code style="background:#f5f5f5;padding:2px 6px;border-radius:3px;font-size:12px;">MEASURE()</code> で呼出</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">ℹ️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">ポイント</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         従来の View は <code>GROUP BY</code> が固定で、切り口を変えるたびに別 View が必要でした。<br/>
# MAGIC         <strong>Metric View は定義だけしてクエリ時に自由に GROUP BY できる</strong>のが最大の違いです。<br/>
# MAGIC         「真実は YAML にある」状態にして、ダッシュボード・Genie・SQL の<strong>数字のブレを防ぐ</strong>のが狙い。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-bottom:2px solid #E0E0E0;padding-bottom:8px;margin:24px 0 12px;">
# MAGIC   <span style="font-size:16px;font-weight:700;color:#1B3139;">アーキテクチャ</span>
# MAGIC </div>
# MAGIC
# MAGIC <div class="mermaid">
# MAGIC flowchart LR
# MAGIC   subgraph Sources["ソーステーブル(Silver 層)"]
# MAGIC     direction TB
# MAGIC     S1["sl_contracts<br/>(成約・粗利)"]
# MAGIC     S2["sl_vehicles<br/>(在庫車両)"]
# MAGIC     S3["sl_inquiries<br/>(商談履歴)"]
# MAGIC     S4["sl_stores<br/>(店舗マスタ)"]
# MAGIC   end
# MAGIC   MV["3つの Metric View<br/>mv_sales / mv_inventory / mv_inquiry<br/>(セマンティックレイヤー)"]
# MAGIC   subgraph Consumers["利用者"]
# MAGIC     direction TB
# MAGIC     C1["AI/BI Dashboard<br/>経営帳票"]
# MAGIC     C2["Genie<br/>自然言語分析"]
# MAGIC     C3["Notebook / SQL<br/>アドホック分析"]
# MAGIC   end
# MAGIC   S1 --> MV
# MAGIC   S2 --> MV
# MAGIC   S3 --> MV
# MAGIC   S4 --> MV
# MAGIC   MV --> C1
# MAGIC   MV --> C2
# MAGIC   MV --> C3
# MAGIC   style Sources fill:#e3f2fd,stroke:#1B5162,stroke-width:2px
# MAGIC   style Consumers fill:#EEF2F7,stroke:#64748B,stroke-width:1.25px
# MAGIC   style MV fill:#FFFFFF,stroke:#FF3621,stroke-width:2px
# MAGIC </div>
# MAGIC
# MAGIC <script type="module">
# MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs";
# MAGIC mermaid.initialize({ startOnLoad: true, theme: "neutral" });
# MAGIC </script>
# MAGIC
# MAGIC <div style="border-bottom:2px solid #E0E0E0;padding-bottom:8px;margin:24px 0 12px;">
# MAGIC   <span style="font-size:16px;font-weight:700;color:#1B3139;">📖 YAML 構造の読み方</span>
# MAGIC </div>
# MAGIC
# MAGIC <table style="width:100%;border-collapse:collapse;margin:12px 0;font-size:14px;">
# MAGIC   <thead>
# MAGIC     <tr style="background:#1B3139;color:#fff;">
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;">フィールド</th>
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;">必須</th>
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;">意味</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background:#fff;"><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-weight:600;">version</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">✅</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">YAML 仕様バージョン。現在は <code>1.1</code> 固定</td></tr>
# MAGIC     <tr style="background:#F8F9FA;"><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-weight:600;">source</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">✅</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">ファクトテーブル</td></tr>
# MAGIC     <tr style="background:#fff;"><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-weight:600;">joins</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">-</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">ディメンションテーブルとの結合定義</td></tr>
# MAGIC     <tr style="background:#F8F9FA;"><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-weight:600;">dimensions</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">✅</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;"><strong>切り口</strong>の一覧</td></tr>
# MAGIC     <tr style="background:#fff;"><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-weight:600;">measures</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">✅</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;"><strong>集計指標</strong>の一覧</td></tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC
# MAGIC <div style="font-size:13px;color:#666;margin:8px 0 4px;">各 dimension / measure には以下のプロパティを設定できます:</div>
# MAGIC
# MAGIC <table style="width:100%;border-collapse:collapse;margin:8px 0 16px;font-size:13px;">
# MAGIC   <thead>
# MAGIC     <tr style="background:#F8F9FA;">
# MAGIC       <th style="padding:8px 12px;text-align:left;font-weight:600;border-bottom:2px solid #E0E0E0;">プロパティ</th>
# MAGIC       <th style="padding:8px 12px;text-align:left;font-weight:600;border-bottom:2px solid #E0E0E0;">必須</th>
# MAGIC       <th style="padding:8px 12px;text-align:left;font-weight:600;border-bottom:2px solid #E0E0E0;">意味</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr><td style="padding:6px 12px;border-bottom:1px solid #eee;font-family:monospace;">name</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">✅</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">識別名。MEASURE() やクエリで使う</td></tr>
# MAGIC     <tr><td style="padding:6px 12px;border-bottom:1px solid #eee;font-family:monospace;">expr</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">✅</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">SQL 式</td></tr>
# MAGIC     <tr><td style="padding:6px 12px;border-bottom:1px solid #eee;font-family:monospace;">display_name</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">-</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">UI/Genie の表示名(日本語 OK)</td></tr>
# MAGIC     <tr><td style="padding:6px 12px;border-bottom:1px solid #eee;font-family:monospace;">synonyms</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">-</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">Genie が認識する別名(日英両方推奨)</td></tr>
# MAGIC     <tr><td style="padding:6px 12px;border-bottom:1px solid #eee;font-family:monospace;">format</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">-</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">表示フォーマット(小数桁・通貨・パーセント)</td></tr>
# MAGIC     <tr><td style="padding:6px 12px;border-bottom:1px solid #eee;font-family:monospace;">comment</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">-</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">説明コメント</td></tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC
# MAGIC <hr style="border:none;border-top:2px solid #E0E0E0;margin:28px 0;"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 販売・粗利 KPI(`mv_sales_kpi`)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="font-size:14px;color:#333;line-height:1.6;margin:12px 0;">
# MAGIC   <code>sl_contracts</code> をファクト、<code>sl_stores</code> / <code>sl_vehicles</code> をディメンションとして JOIN。<br/>
# MAGIC   「いつ・どの店舗で・どの車種カテゴリの・どんな状態の車が」「何台・いくら・粗利率いくらで」売れたかを多軸で集計可能に。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. Catalog Explorer + Genie Code で作成する(メイン)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left:4px solid #ffc107;background:#fffde7;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">🎯</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">ここでやること</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         Catalog Explorer から Genie Code に<strong>日本語で指示</strong>を出して Metric View を作成します。<br/>
# MAGIC         コードを書く必要はありません。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="position:relative;padding-left:36px;margin:16px 0;">
# MAGIC   <div style="position:absolute;left:14px;top:0;bottom:0;width:3px;background:linear-gradient(to bottom,#FF3621,#7B61FF);border-radius:2px;"></div>
# MAGIC
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#FF3621;border:3px solid #fff;box-shadow:0 0 0 2px #FF3621;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 1: Catalog Explorer で <code>sl_contracts</code> テーブルを開く</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">左サイドバー <strong>Catalog</strong> → 00_config のカタログ → スキーマ → <code>sl_contracts</code> を選択</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#FF6F00;border:3px solid #fff;box-shadow:0 0 0 2px #FF6F00;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 2: Create &gt; Metric view をクリック</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">テーブル詳細ページの右上 <strong>Create</strong> ボタン → <strong>Metric view</strong> を選択</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#009688;border:3px solid #fff;box-shadow:0 0 0 2px #009688;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 3: 名前とカタログ・スキーマを指定</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">
# MAGIC         <strong>Name:</strong> <code>mv_sales_kpi</code><br/>
# MAGIC         <strong>Catalog:</strong> 00_config で設定したカタログ名<br/>
# MAGIC         <strong>Schema:</strong> 00_config で設定したスキーマ名<br/>
# MAGIC         → <strong>Create</strong> をクリック
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#AB47BC;border:3px solid #fff;box-shadow:0 0 0 2px #AB47BC;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 4: Genie Code に以下の指示を入力</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">エディタ右上の Genie Code アイコンをクリック → 下の日本語プロンプトをコピペ</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border:2px solid #AB47BC;border-radius:8px;padding:16px 20px;margin:16px 0;background:#faf5ff;">
# MAGIC   <div style="font-weight:700;color:#AB47BC;font-size:13px;margin-bottom:8px;">💬 Genie Code への指示(コピーして貼り付け)</div>
# MAGIC   <div style="font-size:14px;color:#1B3139;line-height:1.8;background:#fff;border:1px solid #e0d4f5;border-radius:6px;padding:12px 16px;">
# MAGIC     中古車の成約データから、店舗の販売・粗利分析に使える Metric View を作ってください。<br/><br/>
# MAGIC     <strong>結合するテーブル:</strong><br/>
# MAGIC     ・sl_stores テーブル(店舗マスタ)を store_id で結合<br/>
# MAGIC     ・sl_vehicles テーブル(車両マスタ)を vehicle_id で結合<br/><br/>
# MAGIC     <strong>分析の切り口:</strong><br/>
# MAGIC     ・契約年月(月単位)、契約日<br/>
# MAGIC     ・店舗名、都道府県、店舗業態<br/>
# MAGIC     ・ボディタイプ、メーカー<br/>
# MAGIC     ・年式区分(現在年からの経過年数で「3年以内」「4-7年」「8-10年」「10年超」)<br/>
# MAGIC     ・認定中古車フラグ、修復歴フラグ<br/><br/>
# MAGIC     <strong>集計したい指標:</strong><br/>
# MAGIC     ・契約件数<br/>
# MAGIC     ・売上合計(actual_price の合計)<br/>
# MAGIC     ・粗利合計(gross_profit の合計)<br/>
# MAGIC     ・平均販売価格<br/>
# MAGIC     ・加重平均粗利率(粗利合計 ÷ 売上合計)※レコード単純平均ではなく金額重み付き<br/>
# MAGIC     ・ユニーク顧客数<br/>
# MAGIC     ・顧客あたり売上(売上合計 ÷ ユニーク顧客数)<br/>
# MAGIC     ・認定中古車販売比率(認定車の契約件数 ÷ 全契約件数)<br/><br/>
# MAGIC     <strong>その他:</strong><br/>
# MAGIC     ・表示名は日本語、Genie で日英両方の同義語(売上 / sales、粗利 / gross profit など)で検索できるように<br/>
# MAGIC     ・金額は小数点なし、粗利率と認定比率はパーセント表示で小数1桁
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="position:relative;padding-left:36px;margin:16px 0;">
# MAGIC   <div style="position:absolute;left:14px;top:0;bottom:0;width:3px;background:linear-gradient(to bottom,#AB47BC,#4caf50);border-radius:2px;"></div>
# MAGIC
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#AB47BC;border:3px solid #fff;box-shadow:0 0 0 2px #AB47BC;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 5: 生成された YAML をレビュー</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">Genie Code が YAML を生成。下の「正解例」と見比べて意図通りか確認</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#4caf50;border:3px solid #fff;box-shadow:0 0 0 2px #4caf50;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 6: 保存</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">右上の <strong>Save</strong> で Metric View を作成</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <details style="margin:16px 0;border:1px solid #E0E0E0;border-radius:8px;overflow:hidden;">
# MAGIC <summary style="padding:12px 16px;background:#fff3e0;cursor:pointer;font-weight:600;font-size:14px;color:#FF6F00;">📋 正解例: 期待される YAML(クリックで展開)</summary>
# MAGIC
# MAGIC <div style="padding:16px;">
# MAGIC <div style="font-size:13px;color:#555;margin-bottom:8px;">Catalog Explorer → Create → Metric view → 以下をエディタに貼り付け<br/><strong>※ source / joins のカタログ名・スキーマ名は、ご自身の環境に合わせて読み替えてください</strong></div>
# MAGIC <button onclick="copyYaml1()">クリップボードにコピー</button>
# MAGIC
# MAGIC <pre id="yaml-block-1" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC version: '1.1'
# MAGIC source: &lt;カタログ名&gt;.&lt;スキーマ名&gt;.sl_contracts
# MAGIC comment: 中古車の成約データに店舗・車両マスタを結合した販売・粗利 KPI Metric View
# MAGIC
# MAGIC joins:
# MAGIC   - name: store
# MAGIC     source: &lt;カタログ名&gt;.&lt;スキーマ名&gt;.sl_stores
# MAGIC     on: source.store_id = store.store_id
# MAGIC   - name: vehicle
# MAGIC     source: &lt;カタログ名&gt;.&lt;スキーマ名&gt;.sl_vehicles
# MAGIC     on: source.vehicle_id = vehicle.vehicle_id
# MAGIC
# MAGIC dimensions:
# MAGIC   - name: 契約年月
# MAGIC     expr: "DATE_TRUNC('MONTH', source.contract_date)"
# MAGIC     display_name: 契約年月
# MAGIC     comment: 契約日を月単位に丸めた日付(月初日)
# MAGIC     synonyms: [月, 年月, contract month, month]
# MAGIC     format:
# MAGIC       type: date
# MAGIC       date_format: year_month_day
# MAGIC   - name: 契約日
# MAGIC     expr: source.contract_date
# MAGIC     display_name: 契約日
# MAGIC     comment: 契約が締結された日付(日次集計用)
# MAGIC     synonyms: [日付, contract date, date]
# MAGIC     format:
# MAGIC       type: date
# MAGIC       date_format: year_month_day
# MAGIC   - name: 店舗名
# MAGIC     expr: store.store_name
# MAGIC     display_name: 店舗名
# MAGIC     comment: 店舗名
# MAGIC     synonyms: [店名, store, store name]
# MAGIC   - name: 都道府県
# MAGIC     expr: store.prefecture
# MAGIC     display_name: 都道府県
# MAGIC     comment: 店舗所在の都道府県
# MAGIC     synonyms: [県, エリア, prefecture, region]
# MAGIC   - name: 店舗業態
# MAGIC     expr: store.format
# MAGIC     display_name: 店舗業態
# MAGIC     comment: 店舗業態(中古車プラザ / 買取専門 / 輸入車専門)
# MAGIC     synonyms: [業態, format, store format, type]
# MAGIC   - name: ボディタイプ
# MAGIC     expr: vehicle.body_type
# MAGIC     display_name: ボディタイプ
# MAGIC     comment: ボディタイプ(セダン / SUV / ミニバン / コンパクト / 軽 / 輸入)
# MAGIC     synonyms: [車種カテゴリ, body type, vehicle type]
# MAGIC   - name: メーカー
# MAGIC     expr: vehicle.maker
# MAGIC     display_name: メーカー
# MAGIC     comment: メーカー名
# MAGIC     synonyms: [ブランド, maker, manufacturer, brand]
# MAGIC   - name: 年式区分
# MAGIC     expr: |-
# MAGIC       CASE
# MAGIC         WHEN (YEAR(current_date()) - vehicle.model_year) <= 3 THEN '3年以内'
# MAGIC         WHEN (YEAR(current_date()) - vehicle.model_year) <= 7 THEN '4-7年'
# MAGIC         WHEN (YEAR(current_date()) - vehicle.model_year) <= 10 THEN '8-10年'
# MAGIC         ELSE '10年超'
# MAGIC       END
# MAGIC     display_name: 年式区分
# MAGIC     comment: 現在年からの経過年数で 4 区分
# MAGIC     synonyms: [年式バケット, year bucket, age bucket]
# MAGIC   - name: 認定中古車
# MAGIC     expr: CASE WHEN vehicle.certified THEN '認定' ELSE '非認定' END
# MAGIC     display_name: 認定中古車
# MAGIC     comment: 認定中古車フラグ(認定 / 非認定)
# MAGIC     synonyms: [認定フラグ, certified, certification]
# MAGIC   - name: 修復歴
# MAGIC     expr: vehicle.repair_history
# MAGIC     display_name: 修復歴
# MAGIC     comment: 修復歴('あり' / 'なし')
# MAGIC     synonyms: [事故歴, repair history, accident]
# MAGIC
# MAGIC measures:
# MAGIC   - name: 契約件数
# MAGIC     expr: COUNT(1)
# MAGIC     display_name: 契約件数
# MAGIC     comment: 成約した契約の件数(台数)
# MAGIC     synonyms: [販売台数, 成約数, contracts, deals, units sold]
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC   - name: 売上合計
# MAGIC     expr: SUM(source.actual_price)
# MAGIC     display_name: 売上合計
# MAGIC     comment: 販売価格の合計(円)
# MAGIC     synonyms: [売上, 総売上, total sales, revenue]
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC   - name: 粗利合計
# MAGIC     expr: SUM(source.gross_profit)
# MAGIC     display_name: 粗利合計
# MAGIC     comment: 粗利の合計(円)
# MAGIC     synonyms: [粗利, total gross profit, gross profit]
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC   - name: 平均販売価格
# MAGIC     expr: AVG(source.actual_price)
# MAGIC     display_name: 平均販売価格
# MAGIC     comment: 1台あたりの平均販売価格(円)
# MAGIC     synonyms: [平均単価, average sale price, avg price]
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC   - name: 加重平均粗利率
# MAGIC     expr: MEASURE(`粗利合計`) / NULLIF(MEASURE(`売上合計`), 0)
# MAGIC     display_name: 加重平均粗利率
# MAGIC     comment: 粗利合計 ÷ 売上合計。レコード単純平均ではなく金額重み付き
# MAGIC     synonyms: [粗利率, gross margin, margin]
# MAGIC     format:
# MAGIC       type: percentage
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 1
# MAGIC   - name: ユニーク顧客数
# MAGIC     expr: COUNT(DISTINCT source.customer_id)
# MAGIC     display_name: ユニーク顧客数
# MAGIC     comment: 期間内の購入顧客のユニーク数
# MAGIC     synonyms: [顧客数, unique customers, customer count]
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC   - name: 顧客あたり売上
# MAGIC     expr: MEASURE(`売上合計`) / NULLIF(MEASURE(`ユニーク顧客数`), 0)
# MAGIC     display_name: 顧客あたり売上
# MAGIC     comment: 売上合計 ÷ ユニーク顧客数。LTV 簡易版
# MAGIC     synonyms: [客単価, sales per customer, ARPU]
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC   - name: 認定中古車販売比率
# MAGIC     expr: "COUNT(1) FILTER (WHERE vehicle.certified = true) / NULLIF(COUNT(1), 0)"
# MAGIC     display_name: 認定中古車販売比率
# MAGIC     comment: 認定中古車の契約件数 ÷ 全契約件数
# MAGIC     synonyms: [認定比率, certified ratio, certification rate]
# MAGIC     format:
# MAGIC       type: percentage
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 1
# MAGIC </code></pre>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function copyYaml1() {
# MAGIC   var el = document.getElementById("yaml-block-1");
# MAGIC   if (!el) return;
# MAGIC   var text = el.innerText;
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text).then(function() { alert("クリップボードにコピーしました"); }).catch(function() { fallbackCopy1(text); });
# MAGIC   } else { fallbackCopy1(text); }
# MAGIC }
# MAGIC function fallbackCopy1(text) {
# MAGIC   var ta = document.createElement("textarea"); ta.value = text; ta.style.position = "fixed"; ta.style.left = "-9999px";
# MAGIC   document.body.appendChild(ta); ta.select();
# MAGIC   try { document.execCommand("copy"); alert("クリップボードにコピーしました"); } catch(e) { alert("コピーできませんでした。手動でコピーしてください。"); }
# MAGIC   document.body.removeChild(ta);
# MAGIC }
# MAGIC </script>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. SQL で作成する場合(オプション)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #607d8b;background:#eceff1;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">⚙️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">SQL での作成はオプションです</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         上の 2a で Catalog Explorer から作成済みの場合、このセルの実行は不要です。<br/>
# MAGIC         再現性が必要な場合や Git 管理したい場合に SQL での作成が便利です。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,オプション: SQL で mv_sales_kpi を作成
# %skip
spark.sql(f"""
CREATE OR REPLACE VIEW {MY_CATALOG}.{MY_SCHEMA}.mv_sales_kpi
WITH METRICS
LANGUAGE YAML
AS $$
version: '1.1'
source: {MY_CATALOG}.{MY_SCHEMA}.sl_contracts
comment: 中古車の成約データに店舗・車両マスタを結合した販売・粗利 KPI Metric View

joins:
  - name: store
    source: {MY_CATALOG}.{MY_SCHEMA}.sl_stores
    on: source.store_id = store.store_id
  - name: vehicle
    source: {MY_CATALOG}.{MY_SCHEMA}.sl_vehicles
    on: source.vehicle_id = vehicle.vehicle_id

dimensions:
  - name: 契約年月
    expr: "DATE_TRUNC('MONTH', source.contract_date)"
    display_name: 契約年月
    comment: 契約日を月単位に丸めた日付(月初日)
    synonyms: [月, 年月, contract month, month]
    format:
      type: date
      date_format: year_month_day
  - name: 契約日
    expr: source.contract_date
    display_name: 契約日
    comment: 契約が締結された日付(日次集計用)
    synonyms: [日付, contract date, date]
    format:
      type: date
      date_format: year_month_day
  - name: 店舗名
    expr: store.store_name
    display_name: 店舗名
    comment: 店舗名
    synonyms: [店名, store, store name]
  - name: 都道府県
    expr: store.prefecture
    display_name: 都道府県
    comment: 店舗所在の都道府県
    synonyms: [県, エリア, prefecture, region]
  - name: 店舗業態
    expr: store.format
    display_name: 店舗業態
    comment: 店舗業態(中古車プラザ / 買取専門 / 輸入車専門)
    synonyms: [業態, format, store format, type]
  - name: ボディタイプ
    expr: vehicle.body_type
    display_name: ボディタイプ
    comment: ボディタイプ(セダン / SUV / ミニバン / コンパクト / 軽 / 輸入)
    synonyms: [車種カテゴリ, body type, vehicle type]
  - name: メーカー
    expr: vehicle.maker
    display_name: メーカー
    comment: メーカー名
    synonyms: [ブランド, maker, manufacturer, brand]
  - name: 年式区分
    expr: |-
      CASE
        WHEN (YEAR(current_date()) - vehicle.model_year) <= 3 THEN '3年以内'
        WHEN (YEAR(current_date()) - vehicle.model_year) <= 7 THEN '4-7年'
        WHEN (YEAR(current_date()) - vehicle.model_year) <= 10 THEN '8-10年'
        ELSE '10年超'
      END
    display_name: 年式区分
    comment: 現在年からの経過年数で 4 区分
    synonyms: [年式バケット, year bucket, age bucket]
  - name: 認定中古車
    expr: CASE WHEN vehicle.certified THEN '認定' ELSE '非認定' END
    display_name: 認定中古車
    comment: 認定中古車フラグ(認定 / 非認定)
    synonyms: [認定フラグ, certified, certification]
  - name: 修復歴
    expr: vehicle.repair_history
    display_name: 修復歴
    comment: 修復歴('あり' / 'なし')
    synonyms: [事故歴, repair history, accident]

measures:
  - name: 契約件数
    expr: COUNT(1)
    display_name: 契約件数
    comment: 成約した契約の件数(台数)
    synonyms: [販売台数, 成約数, contracts, deals, units sold]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 売上合計
    expr: SUM(source.actual_price)
    display_name: 売上合計
    comment: 販売価格の合計(円)
    synonyms: [売上, 総売上, total sales, revenue]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 粗利合計
    expr: SUM(source.gross_profit)
    display_name: 粗利合計
    comment: 粗利の合計(円)
    synonyms: [粗利, total gross profit, gross profit]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 平均販売価格
    expr: AVG(source.actual_price)
    display_name: 平均販売価格
    comment: 1台あたりの平均販売価格(円)
    synonyms: [平均単価, average sale price, avg price]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 加重平均粗利率
    expr: MEASURE(`粗利合計`) / NULLIF(MEASURE(`売上合計`), 0)
    display_name: 加重平均粗利率
    comment: 粗利合計 ÷ 売上合計。レコード単純平均ではなく金額重み付き
    synonyms: [粗利率, gross margin, margin]
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 1
  - name: ユニーク顧客数
    expr: COUNT(DISTINCT source.customer_id)
    display_name: ユニーク顧客数
    comment: 期間内の購入顧客のユニーク数
    synonyms: [顧客数, unique customers, customer count]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 顧客あたり売上
    expr: MEASURE(`売上合計`) / NULLIF(MEASURE(`ユニーク顧客数`), 0)
    display_name: 顧客あたり売上
    comment: 売上合計 ÷ ユニーク顧客数。LTV 簡易版
    synonyms: [客単価, sales per customer, ARPU]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 認定中古車販売比率
    expr: "COUNT(1) FILTER (WHERE vehicle.certified = true) / NULLIF(COUNT(1), 0)"
    display_name: 認定中古車販売比率
    comment: 認定中古車の契約件数 ÷ 全契約件数
    synonyms: [認定比率, certified ratio, certification rate]
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 1
$$
""")
print(f"✅ {MY_CATALOG}.{MY_SCHEMA}.mv_sales_kpi を作成しました")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #4caf50;background:#e8f5e9;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">✅</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">実行結果</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         <code>mv_sales_kpi</code> が作成されました。<br/>
# MAGIC         10 のディメンション(契約年月・店舗・車両属性)と 8 のメジャー(契約件数・売上・粗利・加重平均粗利率など)を定義済み。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,動作確認: 月次×店舗業態の販売サマリ
# MAGIC %sql
# MAGIC SELECT
# MAGIC   `契約年月`,
# MAGIC   `店舗業態`,
# MAGIC   MEASURE(`契約件数`) AS `契約件数`,
# MAGIC   MEASURE(`売上合計`) AS `売上合計`,
# MAGIC   MEASURE(`加重平均粗利率`) AS `加重平均粗利率`
# MAGIC FROM mv_sales_kpi
# MAGIC GROUP BY ALL
# MAGIC ORDER BY `契約年月`, `店舗業態`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">🔍</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">(比較) Metric View を使わない場合の同等 SQL</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         <strong>数値は完全一致</strong>します。違うのは表示フォーマット(桁区切り・%表記・日付形式)だけ。<br/>
# MAGIC         Metric View は YAML の <code>format:</code> 定義で <strong>自動適用</strong>されますが、生 SQL は <code>FORMAT_NUMBER</code> / <code>DATE_FORMAT</code> / <code>* 100</code> &amp; <code>'%'</code> 連結を <strong>毎回書く</strong>必要があります。<br/>
# MAGIC         JOIN・集計式・粗利率の計算もすべて手書きとなり、別クエリ・別ダッシュボードで再利用するたびに記述が散らばって <strong>定義のブレ・修正漏れ</strong> の温床に。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,(参考) Metric View なしの場合の生 SQL
# MAGIC %sql
# MAGIC SELECT
# MAGIC   DATE_FORMAT(DATE_TRUNC('MONTH', c.contract_date), 'yyyy-M-d') AS `契約年月`,
# MAGIC   s.format AS `店舗業態`,
# MAGIC   FORMAT_NUMBER(COUNT(1), 0) AS `契約件数`,
# MAGIC   FORMAT_NUMBER(SUM(c.actual_price), 0) AS `売上合計`,
# MAGIC   CONCAT(FORMAT_NUMBER(SUM(c.gross_profit) / NULLIF(SUM(c.actual_price), 0) * 100, 1), '%') AS `加重平均粗利率`
# MAGIC FROM sl_contracts c
# MAGIC LEFT JOIN sl_stores s
# MAGIC   ON c.store_id = s.store_id
# MAGIC GROUP BY DATE_TRUNC('MONTH', c.contract_date), s.format
# MAGIC ORDER BY DATE_TRUNC('MONTH', c.contract_date), s.format

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 在庫 KPI(`mv_inventory_kpi`)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="font-size:14px;color:#333;line-height:1.6;margin:12px 0;">
# MAGIC   <code>sl_vehicles</code>(在庫車両マスタ)をファクト、<code>sl_stores</code> をディメンションとして JOIN。<br/>
# MAGIC   「どの店舗・どのカテゴリで」「在庫が何台・平均価格いくら・走行距離平均いくら」かを把握。<br/>
# MAGIC   後続のダッシュボードで <strong>「滞留在庫」</strong>の早期発見に使います。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3a. Catalog Explorer + Genie Code で作成する(メイン)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="font-size:14px;color:#333;line-height:1.6;margin:12px 0;">
# MAGIC   セクション 2 と同様、Catalog Explorer で <code>sl_vehicles</code> テーブルを開き、<strong>Create &gt; Metric view</strong> で作成します。
# MAGIC </div>
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:100px 1fr;gap:1px;background:#E0E0E0;border-radius:8px;overflow:hidden;margin:12px 0;font-size:14px;">
# MAGIC   <div style="background:#1B3139;color:#fff;padding:10px 14px;font-weight:600;">Name</div>
# MAGIC   <div style="background:#fff;padding:10px 14px;font-family:monospace;">mv_inventory_kpi</div>
# MAGIC   <div style="background:#1B3139;color:#fff;padding:10px 14px;font-weight:600;">Catalog</div>
# MAGIC   <div style="background:#fff;padding:10px 14px;">00_config で設定したカタログ名</div>
# MAGIC   <div style="background:#1B3139;color:#fff;padding:10px 14px;font-weight:600;">Schema</div>
# MAGIC   <div style="background:#fff;padding:10px 14px;">00_config で設定したスキーマ名</div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border:2px solid #AB47BC;border-radius:8px;padding:16px 20px;margin:16px 0;background:#faf5ff;">
# MAGIC   <div style="font-weight:700;color:#AB47BC;font-size:13px;margin-bottom:8px;">💬 Genie Code への指示(コピーして貼り付け)</div>
# MAGIC   <div style="font-size:14px;color:#1B3139;line-height:1.8;background:#fff;border:1px solid #e0d4f5;border-radius:6px;padding:12px 16px;">
# MAGIC     在庫車両マスタから、店舗別・車種カテゴリ別の在庫分析に使える Metric View を作ってください。<br/><br/>
# MAGIC     <strong>結合するテーブル:</strong><br/>
# MAGIC     ・sl_stores テーブル(店舗マスタ)を store_id で結合<br/><br/>
# MAGIC     <strong>分析の切り口:</strong><br/>
# MAGIC     ・店舗名、都道府県、店舗業態<br/>
# MAGIC     ・ボディタイプ、メーカー<br/>
# MAGIC     ・年式区分(3年以内 / 4-7年 / 8-10年 / 10年超)<br/>
# MAGIC     ・走行距離区分(〜3万km / 3-6万km / 6-10万km / 10万km超)<br/>
# MAGIC     ・認定中古車(認定 / 非認定)、修復歴(あり / なし)<br/><br/>
# MAGIC     <strong>集計したい指標:</strong><br/>
# MAGIC     ・在庫台数<br/>
# MAGIC     ・販売予定価格の合計、仕入価格の合計<br/>
# MAGIC     ・平均販売予定価格、平均走行距離(km)、平均年式<br/>
# MAGIC     ・認定中古車比率、修復歴あり比率(どちらもパーセント表示)<br/><br/>
# MAGIC     <strong>その他:</strong><br/>
# MAGIC     ・表示名は日本語、Genie で日英両方の同義語(在庫 / inventory、走行距離 / mileage など)で検索できるように<br/>
# MAGIC     ・金額・走行距離は小数なし、年式は小数1桁、比率はパーセント小数1桁
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <details style="margin:16px 0;border:1px solid #E0E0E0;border-radius:8px;overflow:hidden;">
# MAGIC <summary style="padding:12px 16px;background:#fff3e0;cursor:pointer;font-weight:600;font-size:14px;color:#FF6F00;">📋 正解例: 期待される YAML(クリックで展開)</summary>
# MAGIC
# MAGIC <div style="padding:16px;">
# MAGIC <div style="font-size:13px;color:#555;margin-bottom:8px;">Catalog Explorer → Create → Metric view → 以下をエディタに貼り付け<br/><strong>※ source / joins のカタログ名・スキーマ名は、ご自身の環境に合わせて読み替えてください</strong></div>
# MAGIC <button onclick="copyYaml2()">クリップボードにコピー</button>
# MAGIC
# MAGIC <pre id="yaml-block-2" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC version: '1.1'
# MAGIC source: &lt;カタログ名&gt;.&lt;スキーマ名&gt;.sl_vehicles
# MAGIC comment: 在庫車両マスタに店舗マスタを結合した在庫分析 Metric View
# MAGIC
# MAGIC joins:
# MAGIC   - name: store
# MAGIC     source: &lt;カタログ名&gt;.&lt;スキーマ名&gt;.sl_stores
# MAGIC     on: source.store_id = store.store_id
# MAGIC
# MAGIC dimensions:
# MAGIC   - name: 店舗名
# MAGIC     expr: store.store_name
# MAGIC     display_name: 店舗名
# MAGIC     comment: 店舗名
# MAGIC     synonyms: [店名, store, store name]
# MAGIC   - name: 都道府県
# MAGIC     expr: store.prefecture
# MAGIC     display_name: 都道府県
# MAGIC     comment: 店舗所在の都道府県
# MAGIC     synonyms: [県, エリア, prefecture, region]
# MAGIC   - name: 店舗業態
# MAGIC     expr: store.format
# MAGIC     display_name: 店舗業態
# MAGIC     comment: 店舗業態(中古車プラザ / 買取専門 / 輸入車専門)
# MAGIC     synonyms: [業態, format, store format]
# MAGIC   - name: ボディタイプ
# MAGIC     expr: source.body_type
# MAGIC     display_name: ボディタイプ
# MAGIC     comment: ボディタイプ(セダン / SUV / ミニバン / コンパクト / 軽 / 輸入)
# MAGIC     synonyms: [車種カテゴリ, body type, vehicle type]
# MAGIC   - name: メーカー
# MAGIC     expr: source.maker
# MAGIC     display_name: メーカー
# MAGIC     comment: メーカー名
# MAGIC     synonyms: [ブランド, maker, manufacturer, brand]
# MAGIC   - name: 年式区分
# MAGIC     expr: |-
# MAGIC       CASE
# MAGIC         WHEN (YEAR(current_date()) - source.model_year) <= 3 THEN '3年以内'
# MAGIC         WHEN (YEAR(current_date()) - source.model_year) <= 7 THEN '4-7年'
# MAGIC         WHEN (YEAR(current_date()) - source.model_year) <= 10 THEN '8-10年'
# MAGIC         ELSE '10年超'
# MAGIC       END
# MAGIC     display_name: 年式区分
# MAGIC     comment: 現在年からの経過年数で 4 区分
# MAGIC     synonyms: [年式バケット, year bucket, age bucket]
# MAGIC   - name: 走行距離区分
# MAGIC     expr: |-
# MAGIC       CASE
# MAGIC         WHEN source.mileage_km < 30000  THEN '〜3万km'
# MAGIC         WHEN source.mileage_km < 60000  THEN '3-6万km'
# MAGIC         WHEN source.mileage_km < 100000 THEN '6-10万km'
# MAGIC         ELSE                                 '10万km超'
# MAGIC       END
# MAGIC     display_name: 走行距離区分
# MAGIC     comment: 走行距離の 4 区分
# MAGIC     synonyms: [走行バケット, mileage bucket]
# MAGIC   - name: 認定中古車
# MAGIC     expr: CASE WHEN source.certified THEN '認定' ELSE '非認定' END
# MAGIC     display_name: 認定中古車
# MAGIC     comment: 認定中古車フラグ(認定 / 非認定)
# MAGIC     synonyms: [認定フラグ, certified, certification]
# MAGIC   - name: 修復歴
# MAGIC     expr: source.repair_history
# MAGIC     display_name: 修復歴
# MAGIC     comment: 修復歴('あり' / 'なし')
# MAGIC     synonyms: [事故歴, repair history, accident]
# MAGIC
# MAGIC measures:
# MAGIC   - name: 在庫台数
# MAGIC     expr: COUNT(1)
# MAGIC     display_name: 在庫台数
# MAGIC     comment: 在庫車両の台数
# MAGIC     synonyms: [在庫数, inventory count, stock]
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC   - name: 販売予定価格合計
# MAGIC     expr: SUM(source.listing_price)
# MAGIC     display_name: 販売予定価格合計
# MAGIC     comment: listing_price の合計(円)
# MAGIC     synonyms: [掲載価格合計, total listing price]
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC   - name: 仕入価格合計
# MAGIC     expr: SUM(source.purchase_price)
# MAGIC     display_name: 仕入価格合計
# MAGIC     comment: purchase_price の合計(円)
# MAGIC     synonyms: [仕入合計, total purchase price]
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC   - name: 平均販売予定価格
# MAGIC     expr: AVG(source.listing_price)
# MAGIC     display_name: 平均販売予定価格
# MAGIC     comment: 1台あたりの平均販売予定価格(円)
# MAGIC     synonyms: [平均掲載価格, average listing price]
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC   - name: 平均走行距離
# MAGIC     expr: AVG(source.mileage_km)
# MAGIC     display_name: 平均走行距離
# MAGIC     comment: 1台あたりの平均走行距離(km)
# MAGIC     synonyms: [平均ミレージ, average mileage, mileage]
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC   - name: 平均年式
# MAGIC     expr: AVG(source.model_year)
# MAGIC     display_name: 平均年式
# MAGIC     comment: 1台あたりの平均年式(西暦)
# MAGIC     synonyms: [平均モデルイヤー, average model year]
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 1
# MAGIC   - name: 認定中古車比率
# MAGIC     expr: "COUNT(1) FILTER (WHERE source.certified = true) / NULLIF(COUNT(1), 0)"
# MAGIC     display_name: 認定中古車比率
# MAGIC     comment: 在庫に占める認定中古車の比率
# MAGIC     synonyms: [認定比率, certified ratio]
# MAGIC     format:
# MAGIC       type: percentage
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 1
# MAGIC   - name: 修復歴あり比率
# MAGIC     expr: "COUNT(1) FILTER (WHERE source.repair_history = 'あり') / NULLIF(COUNT(1), 0)"
# MAGIC     display_name: 修復歴あり比率
# MAGIC     comment: 在庫に占める修復歴ありの比率
# MAGIC     synonyms: [事故歴比率, repair history ratio]
# MAGIC     format:
# MAGIC       type: percentage
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 1
# MAGIC </code></pre>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function copyYaml2() {
# MAGIC   var el = document.getElementById("yaml-block-2");
# MAGIC   if (!el) return;
# MAGIC   var text = el.innerText;
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text).then(function() { alert("クリップボードにコピーしました"); }).catch(function() { fallbackCopy2(text); });
# MAGIC   } else { fallbackCopy2(text); }
# MAGIC }
# MAGIC function fallbackCopy2(text) {
# MAGIC   var ta = document.createElement("textarea"); ta.value = text; ta.style.position = "fixed"; ta.style.left = "-9999px";
# MAGIC   document.body.appendChild(ta); ta.select();
# MAGIC   try { document.execCommand("copy"); alert("クリップボードにコピーしました"); } catch(e) { alert("コピーできませんでした。手動でコピーしてください。"); }
# MAGIC   document.body.removeChild(ta);
# MAGIC }
# MAGIC </script>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b. SQL で作成する場合(オプション)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #607d8b;background:#eceff1;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">⚙️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">SQL での作成はオプションです</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         上の 3a で Catalog Explorer から作成済みの場合、このセルの実行は不要です。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,オプション: SQL で mv_inventory_kpi を作成
# %skip
spark.sql(f"""
CREATE OR REPLACE VIEW {MY_CATALOG}.{MY_SCHEMA}.mv_inventory_kpi
WITH METRICS
LANGUAGE YAML
AS $$
version: '1.1'
source: {MY_CATALOG}.{MY_SCHEMA}.sl_vehicles
comment: 在庫車両マスタに店舗マスタを結合した在庫分析 Metric View

joins:
  - name: store
    source: {MY_CATALOG}.{MY_SCHEMA}.sl_stores
    on: source.store_id = store.store_id

dimensions:
  - name: 店舗名
    expr: store.store_name
    display_name: 店舗名
    comment: 店舗名
    synonyms: [店名, store, store name]
  - name: 都道府県
    expr: store.prefecture
    display_name: 都道府県
    comment: 店舗所在の都道府県
    synonyms: [県, エリア, prefecture, region]
  - name: 店舗業態
    expr: store.format
    display_name: 店舗業態
    comment: 店舗業態(中古車プラザ / 買取専門 / 輸入車専門)
    synonyms: [業態, format, store format]
  - name: ボディタイプ
    expr: source.body_type
    display_name: ボディタイプ
    comment: ボディタイプ(セダン / SUV / ミニバン / コンパクト / 軽 / 輸入)
    synonyms: [車種カテゴリ, body type, vehicle type]
  - name: メーカー
    expr: source.maker
    display_name: メーカー
    comment: メーカー名
    synonyms: [ブランド, maker, manufacturer, brand]
  - name: 年式区分
    expr: |-
      CASE
        WHEN (YEAR(current_date()) - source.model_year) <= 3 THEN '3年以内'
        WHEN (YEAR(current_date()) - source.model_year) <= 7 THEN '4-7年'
        WHEN (YEAR(current_date()) - source.model_year) <= 10 THEN '8-10年'
        ELSE '10年超'
      END
    display_name: 年式区分
    comment: 現在年からの経過年数で 4 区分
    synonyms: [年式バケット, year bucket, age bucket]
  - name: 走行距離区分
    expr: |-
      CASE
        WHEN source.mileage_km < 30000  THEN '〜3万km'
        WHEN source.mileage_km < 60000  THEN '3-6万km'
        WHEN source.mileage_km < 100000 THEN '6-10万km'
        ELSE                                 '10万km超'
      END
    display_name: 走行距離区分
    comment: 走行距離の 4 区分
    synonyms: [走行バケット, mileage bucket]
  - name: 認定中古車
    expr: CASE WHEN source.certified THEN '認定' ELSE '非認定' END
    display_name: 認定中古車
    comment: 認定中古車フラグ(認定 / 非認定)
    synonyms: [認定フラグ, certified, certification]
  - name: 修復歴
    expr: source.repair_history
    display_name: 修復歴
    comment: 修復歴('あり' / 'なし')
    synonyms: [事故歴, repair history, accident]

measures:
  - name: 在庫台数
    expr: COUNT(1)
    display_name: 在庫台数
    comment: 在庫車両の台数
    synonyms: [在庫数, inventory count, stock]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 販売予定価格合計
    expr: SUM(source.listing_price)
    display_name: 販売予定価格合計
    comment: listing_price の合計(円)
    synonyms: [掲載価格合計, total listing price]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 仕入価格合計
    expr: SUM(source.purchase_price)
    display_name: 仕入価格合計
    comment: purchase_price の合計(円)
    synonyms: [仕入合計, total purchase price]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 平均販売予定価格
    expr: AVG(source.listing_price)
    display_name: 平均販売予定価格
    comment: 1台あたりの平均販売予定価格(円)
    synonyms: [平均掲載価格, average listing price]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 平均走行距離
    expr: AVG(source.mileage_km)
    display_name: 平均走行距離
    comment: 1台あたりの平均走行距離(km)
    synonyms: [平均ミレージ, average mileage, mileage]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 平均年式
    expr: AVG(source.model_year)
    display_name: 平均年式
    comment: 1台あたりの平均年式(西暦)
    synonyms: [平均モデルイヤー, average model year]
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
  - name: 認定中古車比率
    expr: "COUNT(1) FILTER (WHERE source.certified = true) / NULLIF(COUNT(1), 0)"
    display_name: 認定中古車比率
    comment: 在庫に占める認定中古車の比率
    synonyms: [認定比率, certified ratio]
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 1
  - name: 修復歴あり比率
    expr: "COUNT(1) FILTER (WHERE source.repair_history = 'あり') / NULLIF(COUNT(1), 0)"
    display_name: 修復歴あり比率
    comment: 在庫に占める修復歴ありの比率
    synonyms: [事故歴比率, repair history ratio]
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 1
$$
""")
print(f"✅ {MY_CATALOG}.{MY_SCHEMA}.mv_inventory_kpi を作成しました")

# COMMAND ----------

# DBTITLE 1,動作確認: 都道府県別 在庫サマリ
# MAGIC %sql
# MAGIC SELECT
# MAGIC   `都道府県`,
# MAGIC   MEASURE(`在庫台数`) AS `在庫台数`,
# MAGIC   MEASURE(`平均販売予定価格`) AS `平均販売予定価格`,
# MAGIC   MEASURE(`認定中古車比率`) AS `認定中古車比率`,
# MAGIC   MEASURE(`修復歴あり比率`) AS `修復歴あり比率`
# MAGIC FROM mv_inventory_kpi
# MAGIC GROUP BY ALL
# MAGIC ORDER BY `在庫台数` DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">🔍</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">(比較) Metric View を使わない場合の同等 SQL</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         数値は完全一致 — 違いは表示フォーマット(桁区切り・%表記)だけ。<br/>
# MAGIC         Metric View は YAML の <code>format:</code> 定義で <strong>自動適用</strong>されますが、生 SQL は <code>FORMAT_NUMBER</code> &amp; <code>'%'</code> 連結を <strong>毎回書く</strong>必要があります。<br/>
# MAGIC         さらに、<code>FILTER (WHERE ...)</code> 付きの比率計算や JOIN を毎回書くと、「修復歴あり」の判定条件がクエリ間でズレるリスクも(例: <code>= 'あり'</code> vs <code>IS NOT NULL</code>)。Metric View なら<strong>定義 + 表示フォーマットを一箇所に集約</strong>できます。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,(参考) Metric View なしの場合の生 SQL
# MAGIC %sql
# MAGIC SELECT
# MAGIC   s.prefecture AS `都道府県`,
# MAGIC   FORMAT_NUMBER(COUNT(1), 0) AS `在庫台数`,
# MAGIC   FORMAT_NUMBER(AVG(v.listing_price), 0) AS `平均販売予定価格`,
# MAGIC   CONCAT(FORMAT_NUMBER(COUNT(1) FILTER (WHERE v.certified = true) * 100.0 / NULLIF(COUNT(1), 0), 1), '%') AS `認定中古車比率`,
# MAGIC   CONCAT(FORMAT_NUMBER(COUNT(1) FILTER (WHERE v.repair_history = 'あり') * 100.0 / NULLIF(COUNT(1), 0), 1), '%') AS `修復歴あり比率`
# MAGIC FROM sl_vehicles v
# MAGIC LEFT JOIN sl_stores s
# MAGIC   ON v.store_id = s.store_id
# MAGIC GROUP BY s.prefecture
# MAGIC ORDER BY COUNT(1) DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 商談・成約率 KPI(`mv_inquiry_kpi`)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="font-size:14px;color:#333;line-height:1.6;margin:12px 0;">
# MAGIC   <code>sl_inquiries</code>(商談履歴)をファクト、<code>sl_stores</code> をディメンションとして JOIN。<br/>
# MAGIC   「どの店舗・どの来店区分・どのチャネルから」「商談が何件・成約が何件・成約率いくら」かを把握。<br/>
# MAGIC   <code>FILTER (WHERE status = '成約')</code> で派生メジャーを定義し、<strong>成約率</strong>を式で表現します。
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">ℹ️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">FILTER 付きメジャーのポイント</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         <code>SUM(...) FILTER (WHERE ...)</code> や <code>COUNT(1) FILTER (WHERE ...)</code> を使うと、ベースメジャーに条件を付けた派生指標を簡潔に定義できます。<br/>
# MAGIC         同じ商談データから「成約のみ」「失注のみ」「進行中のみ」を分離し、さらに <code>MEASURE(...)</code> 同士を割って成約率も派生定義できます。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4a. Catalog Explorer + Genie Code で作成する(メイン)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:100px 1fr;gap:1px;background:#E0E0E0;border-radius:8px;overflow:hidden;margin:12px 0;font-size:14px;">
# MAGIC   <div style="background:#1B3139;color:#fff;padding:10px 14px;font-weight:600;">Name</div>
# MAGIC   <div style="background:#fff;padding:10px 14px;font-family:monospace;">mv_inquiry_kpi</div>
# MAGIC   <div style="background:#1B3139;color:#fff;padding:10px 14px;font-weight:600;">Catalog</div>
# MAGIC   <div style="background:#fff;padding:10px 14px;">00_config で設定したカタログ名</div>
# MAGIC   <div style="background:#1B3139;color:#fff;padding:10px 14px;font-weight:600;">Schema</div>
# MAGIC   <div style="background:#fff;padding:10px 14px;">00_config で設定したスキーマ名</div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border:2px solid #AB47BC;border-radius:8px;padding:16px 20px;margin:16px 0;background:#faf5ff;">
# MAGIC   <div style="font-weight:700;color:#AB47BC;font-size:13px;margin-bottom:8px;">💬 Genie Code への指示(コピーして貼り付け)</div>
# MAGIC   <div style="font-size:14px;color:#1B3139;line-height:1.8;background:#fff;border:1px solid #e0d4f5;border-radius:6px;padding:12px 16px;">
# MAGIC     中古車の商談履歴データから、店舗別・来店チャネル別の成約率分析に使える Metric View を作ってください。<br/><br/>
# MAGIC     <strong>結合するテーブル:</strong><br/>
# MAGIC     ・sl_stores テーブル(店舗マスタ)を store_id で結合<br/><br/>
# MAGIC     <strong>分析の切り口:</strong><br/>
# MAGIC     ・商談年月、商談日<br/>
# MAGIC     ・店舗名、都道府県、店舗業態<br/>
# MAGIC     ・来店チャネル(channel: 来店 / オンライン / 買取査定 / 電話)<br/>
# MAGIC     ・商談ステータス(status: 進行中 / 成約 / 失注)<br/><br/>
# MAGIC     <strong>集計したい指標:</strong><br/>
# MAGIC     ・商談件数<br/>
# MAGIC     ・成約商談数(status='成約' のみカウント)<br/>
# MAGIC     ・失注商談数(status='失注' のみカウント)<br/>
# MAGIC     ・成約率(成約商談数 ÷ 商談件数、パーセント表示)<br/>
# MAGIC     ・ユニーク顧客数(商談に来た顧客のユニーク数)<br/><br/>
# MAGIC     <strong>その他:</strong><br/>
# MAGIC     ・FILTER (WHERE ...) を使って派生メジャーを定義する<br/>
# MAGIC     ・成約率は MEASURE() 同士を割って定義する<br/>
# MAGIC     ・表示名は日本語、Genie で日英両方の同義語(成約 / closed、失注 / lost、成約率 / close rate)で検索できるように<br/>
# MAGIC     ・件数は小数なし、成約率はパーセント小数1桁
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <details style="margin:16px 0;border:1px solid #E0E0E0;border-radius:8px;overflow:hidden;">
# MAGIC <summary style="padding:12px 16px;background:#fff3e0;cursor:pointer;font-weight:600;font-size:14px;color:#FF6F00;">📋 正解例: 期待される YAML(クリックで展開)</summary>
# MAGIC
# MAGIC <div style="padding:16px;">
# MAGIC <div style="font-size:13px;color:#555;margin-bottom:8px;">Catalog Explorer → Create → Metric view → 以下をエディタに貼り付け<br/><strong>※ source / joins のカタログ名・スキーマ名は、ご自身の環境に合わせて読み替えてください</strong></div>
# MAGIC <button onclick="copyYaml3()">クリップボードにコピー</button>
# MAGIC
# MAGIC <pre id="yaml-block-3" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC version: '1.1'
# MAGIC source: &lt;カタログ名&gt;.&lt;スキーマ名&gt;.sl_inquiries
# MAGIC comment: 商談履歴に店舗マスタを結合した来店・成約率分析 Metric View
# MAGIC
# MAGIC joins:
# MAGIC   - name: store
# MAGIC     source: &lt;カタログ名&gt;.&lt;スキーマ名&gt;.sl_stores
# MAGIC     on: source.store_id = store.store_id
# MAGIC
# MAGIC dimensions:
# MAGIC   - name: 商談年月
# MAGIC     expr: "DATE_TRUNC('MONTH', source.inquiry_date)"
# MAGIC     display_name: 商談年月
# MAGIC     comment: 商談日を月単位に丸めた日付(月初日)
# MAGIC     synonyms: [月, 年月, inquiry month, month]
# MAGIC     format:
# MAGIC       type: date
# MAGIC       date_format: year_month_day
# MAGIC   - name: 商談日
# MAGIC     expr: source.inquiry_date
# MAGIC     display_name: 商談日
# MAGIC     comment: 商談が行われた日付(日次集計用)
# MAGIC     synonyms: [日付, inquiry date, date]
# MAGIC     format:
# MAGIC       type: date
# MAGIC       date_format: year_month_day
# MAGIC   - name: 店舗名
# MAGIC     expr: store.store_name
# MAGIC     display_name: 店舗名
# MAGIC     comment: 店舗名
# MAGIC     synonyms: [店名, store, store name]
# MAGIC   - name: 都道府県
# MAGIC     expr: store.prefecture
# MAGIC     display_name: 都道府県
# MAGIC     comment: 店舗所在の都道府県
# MAGIC     synonyms: [県, エリア, prefecture, region]
# MAGIC   - name: 店舗業態
# MAGIC     expr: store.format
# MAGIC     display_name: 店舗業態
# MAGIC     comment: 店舗業態(中古車プラザ / 買取専門 / 輸入車専門)
# MAGIC     synonyms: [業態, format, store format]
# MAGIC   - name: 来店チャネル
# MAGIC     expr: source.channel
# MAGIC     display_name: 来店チャネル
# MAGIC     comment: 来店区分(来店 / オンライン / 買取査定 / 電話)
# MAGIC     synonyms: [チャネル, channel, inquiry channel]
# MAGIC   - name: 商談ステータス
# MAGIC     expr: source.status
# MAGIC     display_name: 商談ステータス
# MAGIC     comment: 商談の進行状況(進行中 / 成約 / 失注)
# MAGIC     synonyms: [ステータス, status, deal status]
# MAGIC
# MAGIC measures:
# MAGIC   - name: 商談件数
# MAGIC     expr: COUNT(1)
# MAGIC     display_name: 商談件数
# MAGIC     comment: 商談の件数
# MAGIC     synonyms: [来店数, inquiries, leads]
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC   - name: 成約商談数
# MAGIC     expr: "COUNT(1) FILTER (WHERE source.status = '成約')"
# MAGIC     display_name: 成約商談数
# MAGIC     comment: 成約に至った商談の件数
# MAGIC     synonyms: [成約数, closed inquiries, deals won]
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC   - name: 失注商談数
# MAGIC     expr: "COUNT(1) FILTER (WHERE source.status = '失注')"
# MAGIC     display_name: 失注商談数
# MAGIC     comment: 失注した商談の件数
# MAGIC     synonyms: [失注数, lost inquiries, deals lost]
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC   - name: 成約率
# MAGIC     expr: MEASURE(`成約商談数`) / NULLIF(MEASURE(`商談件数`), 0)
# MAGIC     display_name: 成約率
# MAGIC     comment: 成約商談数 ÷ 商談件数
# MAGIC     synonyms: [クロージングレート, close rate, win rate, conversion rate]
# MAGIC     format:
# MAGIC       type: percentage
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 1
# MAGIC   - name: ユニーク顧客数
# MAGIC     expr: COUNT(DISTINCT source.customer_id)
# MAGIC     display_name: ユニーク顧客数
# MAGIC     comment: 商談に来たユニーク顧客数
# MAGIC     synonyms: [顧客数, unique customers, customer count]
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC </code></pre>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function copyYaml3() {
# MAGIC   var el = document.getElementById("yaml-block-3");
# MAGIC   if (!el) return;
# MAGIC   var text = el.innerText;
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text).then(function() { alert("クリップボードにコピーしました"); }).catch(function() { fallbackCopy3(text); });
# MAGIC   } else { fallbackCopy3(text); }
# MAGIC }
# MAGIC function fallbackCopy3(text) {
# MAGIC   var ta = document.createElement("textarea"); ta.value = text; ta.style.position = "fixed"; ta.style.left = "-9999px";
# MAGIC   document.body.appendChild(ta); ta.select();
# MAGIC   try { document.execCommand("copy"); alert("クリップボードにコピーしました"); } catch(e) { alert("コピーできませんでした。手動でコピーしてください。"); }
# MAGIC   document.body.removeChild(ta);
# MAGIC }
# MAGIC </script>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. SQL で作成する場合(オプション)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #607d8b;background:#eceff1;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">⚙️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">SQL での作成はオプションです</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         上の 4a で Catalog Explorer から作成済みの場合、このセルの実行は不要です。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,オプション: SQL で mv_inquiry_kpi を作成
# %skip
spark.sql(f"""
CREATE OR REPLACE VIEW {MY_CATALOG}.{MY_SCHEMA}.mv_inquiry_kpi
WITH METRICS
LANGUAGE YAML
AS $$
version: '1.1'
source: {MY_CATALOG}.{MY_SCHEMA}.sl_inquiries
comment: 商談履歴に店舗マスタを結合した来店・成約率分析 Metric View

joins:
  - name: store
    source: {MY_CATALOG}.{MY_SCHEMA}.sl_stores
    on: source.store_id = store.store_id

dimensions:
  - name: 商談年月
    expr: "DATE_TRUNC('MONTH', source.inquiry_date)"
    display_name: 商談年月
    comment: 商談日を月単位に丸めた日付(月初日)
    synonyms: [月, 年月, inquiry month, month]
    format:
      type: date
      date_format: year_month_day
  - name: 商談日
    expr: source.inquiry_date
    display_name: 商談日
    comment: 商談が行われた日付(日次集計用)
    synonyms: [日付, inquiry date, date]
    format:
      type: date
      date_format: year_month_day
  - name: 店舗名
    expr: store.store_name
    display_name: 店舗名
    comment: 店舗名
    synonyms: [店名, store, store name]
  - name: 都道府県
    expr: store.prefecture
    display_name: 都道府県
    comment: 店舗所在の都道府県
    synonyms: [県, エリア, prefecture, region]
  - name: 店舗業態
    expr: store.format
    display_name: 店舗業態
    comment: 店舗業態(中古車プラザ / 買取専門 / 輸入車専門)
    synonyms: [業態, format, store format]
  - name: 来店チャネル
    expr: source.channel
    display_name: 来店チャネル
    comment: 来店区分(来店 / オンライン / 買取査定 / 電話)
    synonyms: [チャネル, channel, inquiry channel]
  - name: 商談ステータス
    expr: source.status
    display_name: 商談ステータス
    comment: 商談の進行状況(進行中 / 成約 / 失注)
    synonyms: [ステータス, status, deal status]

measures:
  - name: 商談件数
    expr: COUNT(1)
    display_name: 商談件数
    comment: 商談の件数
    synonyms: [来店数, inquiries, leads]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 成約商談数
    expr: "COUNT(1) FILTER (WHERE source.status = '成約')"
    display_name: 成約商談数
    comment: 成約に至った商談の件数
    synonyms: [成約数, closed inquiries, deals won]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 失注商談数
    expr: "COUNT(1) FILTER (WHERE source.status = '失注')"
    display_name: 失注商談数
    comment: 失注した商談の件数
    synonyms: [失注数, lost inquiries, deals lost]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 成約率
    expr: MEASURE(`成約商談数`) / NULLIF(MEASURE(`商談件数`), 0)
    display_name: 成約率
    comment: 成約商談数 ÷ 商談件数
    synonyms: [クロージングレート, close rate, win rate, conversion rate]
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 1
  - name: ユニーク顧客数
    expr: COUNT(DISTINCT source.customer_id)
    display_name: ユニーク顧客数
    comment: 商談に来たユニーク顧客数
    synonyms: [顧客数, unique customers, customer count]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
$$
""")
print(f"✅ {MY_CATALOG}.{MY_SCHEMA}.mv_inquiry_kpi を作成しました")

# COMMAND ----------

# DBTITLE 1,動作確認: 来店チャネル別 成約率
# MAGIC %sql
# MAGIC SELECT
# MAGIC   `来店チャネル`,
# MAGIC   MEASURE(`商談件数`) AS `商談件数`,
# MAGIC   MEASURE(`成約商談数`) AS `成約商談数`,
# MAGIC   MEASURE(`成約率`) AS `成約率`
# MAGIC FROM mv_inquiry_kpi
# MAGIC GROUP BY ALL
# MAGIC ORDER BY `成約率` DESC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">🔍</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">(比較) Metric View を使わない場合の同等 SQL</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         数値は完全一致 — 違いは表示フォーマット(桁区切り・%表記)だけ。<br/>
# MAGIC         Metric View は YAML の <code>format:</code> 定義で <strong>自動適用</strong>されますが、生 SQL は <code>FORMAT_NUMBER</code> &amp; <code>'%'</code> 連結を <strong>毎回書く</strong>必要があります。<br/>
# MAGIC         さらに「成約率」のような派生メジャーは Metric View なら <code>MEASURE(`成約商談数`) / MEASURE(`商談件数`)</code> と一行で書けますが、生 SQL では <strong>分子・分母の集計式をクエリのたびに書き直す</strong>必要があります。クエリ・ダッシュボード・Genie で定義がブレる温床。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,(参考) Metric View なしの場合の生 SQL
# MAGIC %sql
# MAGIC SELECT
# MAGIC   i.channel AS `来店チャネル`,
# MAGIC   FORMAT_NUMBER(COUNT(1), 0) AS `商談件数`,
# MAGIC   FORMAT_NUMBER(COUNT(1) FILTER (WHERE i.status = '成約'), 0) AS `成約商談数`,
# MAGIC   CONCAT(FORMAT_NUMBER(COUNT(1) FILTER (WHERE i.status = '成約') * 100.0 / NULLIF(COUNT(1), 0), 1), '%') AS `成約率`
# MAGIC FROM sl_inquiries i
# MAGIC GROUP BY i.channel
# MAGIC ORDER BY COUNT(1) FILTER (WHERE i.status = '成約') * 1.0 / NULLIF(COUNT(1), 0) DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 作成した Metric View 一覧

# COMMAND ----------

# DBTITLE 1,Metric View 一覧
spark.sql(f"""
SHOW VIEWS IN {MY_CATALOG}.{MY_SCHEMA} LIKE 'mv_*'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #4caf50;background:#e8f5e9;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">✅</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">このノートブックで学んだこと</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         <strong>3 つの Metric View を作成しました:</strong>
# MAGIC         <ul style="margin:8px 0 0;padding-left:20px;">
# MAGIC           <li><code>mv_sales_kpi</code> — 販売・粗利 KPI(契約・売上・粗利・加重平均粗利率など)</li>
# MAGIC           <li><code>mv_inventory_kpi</code> — 在庫 KPI(在庫台数・平均価格・平均走行距離・認定比率など)</li>
# MAGIC           <li><code>mv_inquiry_kpi</code> — 商談・成約率 KPI(FILTER 付き派生メジャー + MEASURE() 同士の比率)</li>
# MAGIC         </ul>
# MAGIC         <div style="margin-top:8px;">
# MAGIC           次のノートブックでは、これらの Metric View をデータソースとして <strong>AI/BI ダッシュボード / Genie</strong> から参照し、KPI 定義の重複排除・ブレ防止を実現します。
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #1976D2; background:#E3F2FD; padding:12px 16px; border-radius:4px; margin:10px 0;">
# MAGIC <strong>💡 Metric View のクエリ作法(おさらい)</strong><br>
# MAGIC <ul style="margin:6px 0 0 0;">
# MAGIC   <li><code>SELECT *</code> は <b>不可</b> — ディメンションを明示し、メジャーは <code>MEASURE(`name`)</code> で呼ぶ</li>
# MAGIC   <li>名前にスペース・日本語を含む場合は <b>バッククォート</b>で囲む(例: <code>`契約年月`</code>)</li>
# MAGIC   <li><code>GROUP BY ALL</code> でディメンションを自動グルーピング</li>
# MAGIC   <li>クエリ時の WHERE / ORDER BY / LIMIT は通常通り使える</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #F57C00; background:#FFF3E0; padding:12px 16px; border-radius:4px; margin:10px 0;">
# MAGIC <strong>⚠️ トラブルシューティング</strong><br>
# MAGIC <b>「YAML version not supported」</b>: DBR 17.2+ または サーバーレス SQL を使用してください<br>
# MAGIC <b>「Cannot resolve column」</b>: ディメンション・メジャー名にスペース / 日本語がある場合、クエリでバッククォートが必要<br>
# MAGIC <b>「SELECT * not supported」</b>: Metric View では <code>MEASURE()</code> を使った明示クエリのみ許可<br>
# MAGIC <b>「Source table not found」</b>: 03 のノートブックを実行して <code>sl_*</code> テーブルを作成してから再実行<br>
# MAGIC <b>「No such struct field」</b>: JOIN エイリアス経由のカラム参照が間違っている。<code>sl_vehicles</code> の認定フラグは <code>certified</code>、修復歴は <code>repair_history</code>(<code>is_certified</code> / <code>has_repair_history</code> ではない)
# MAGIC </div>
