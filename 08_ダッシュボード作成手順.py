# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 08 | AI/BI ダッシュボード作成手順
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">AI にプロンプトを渡すだけ — ノーコードで中古車販売ダッシュボードを作成する</p>
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
# MAGIC AI/BI ダッシュボードの <b>エージェントモード(Genie Code)</b> を使って、プロンプト 1 つでダッシュボードを自動生成します。<br>
# MAGIC 06 で作成した <b>UC Metric Views</b> をデータソースに、テーブルの内容を AI が理解し、<br>
# MAGIC 適切なチャートとレイアウトを提案してくれることを体験しましょう！
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📋 前提条件</strong><br>
# MAGIC ✅ <code>05_テーブル設定.py</code> 実行済み(コメント・PK/FK 設定済み — AI の生成精度向上に必須)<br>
# MAGIC ✅ <code>06_UC_Metrics_Views.py</code> 実行済み(<code>mv_sales_kpi</code> / <code>mv_inventory_kpi</code> / <code>mv_inquiry_kpi</code> 作成済み)<br>
# MAGIC ✅ <code>07_Genie作成手順.py</code> 完了(任意 — Genie 連携を体験する場合)<br>
# MAGIC ✅ サーバーレス SQL ウェアハウスが起動中
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,共通設定の読み込み
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 1. AI/BI ダッシュボードとは
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">1. AI/BI ダッシュボードとは</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 AI/BI ダッシュボードとは</strong><br>
# MAGIC Databricks のノーコード BI ツールです。
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li>SQL で定義したデータセットを元に、ドラッグ & ドロップでチャートを作成</li>
# MAGIC   <li><b>Genie Code</b> にプロンプトを渡すと、データセット・チャート・レイアウトを自動生成</li>
# MAGIC   <li>フィルタ・パラメータで動的に絞り込み可能</li>
# MAGIC   <li>公開してチームメンバーと共有可能</li>
# MAGIC   <li><b>UC Metric Views</b> を直接データソースに指定でき、業務指標の定義が一元管理される</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 2. ダッシュボードの作成手順
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">2. ダッシュボードの作成手順</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: AI エージェントでダッシュボードを作成する</strong><br><br>
# MAGIC <strong>手順:</strong>
# MAGIC <ol style="margin: 6px 0 0 0;">
# MAGIC   <li>左メニューの「<b>Dashboards</b>」をクリック</li>
# MAGIC   <li>右上の「<b>Create dashboard</b>」をクリック</li>
# MAGIC   <li>空のダッシュボードが開いたら、右下の <b>Genie Code(💬 アイコン)</b> をクリック</li>
# MAGIC   <li>下のセルに表示される <b>プロンプトをコピー</b> して貼り付け</li>
# MAGIC   <li>AI がデータセット・チャート・レイアウトを自動生成するのを待つ(2〜3 分)</li>
# MAGIC   <li>生成されたダッシュボードを確認し、必要に応じて調整</li>
# MAGIC </ol>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ プロンプト A(シンプル版) — まずはこちらを試してください</strong><br>
# MAGIC スキーマ名だけ渡して、AI にお任せでダッシュボードを作らせます。<br>
# MAGIC コメント・PK/FK・Metric Views が整備済みなので、これだけでもそれなりに動きます。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,プロンプト A: シンプル版
prompt_simple = f"""{MY_CATALOG}.{MY_SCHEMA} スキーマ配下のテーブルと Metric Views を使って、中古車販売チェーンの本部・店長が使う経営ダッシュボードを作って"""

print(prompt_simple)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 シンプル版のポイント</strong><br>
# MAGIC テーブルにコメント・PK/FK 制約を設定済み(05)、業務指標は Metric Views(06)で定義済みなので、<br>
# MAGIC スキーマ名を渡すだけでも AI がデータの意味を理解し、それなりのダッシュボードを作ってくれます。<br>
# MAGIC ただし、チャートの種類やレイアウトは AI 任せになるため、意図と違う結果になることもあります。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ プロンプト B(詳細指定版) — より具体的に指示する場合はこちら</strong><br>
# MAGIC チャートの種類やレイアウトまで指定すると、意図に近いダッシュボードが生成されます。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,プロンプト B: 詳細指定版
prompt = f"""以下のテーブル・Metric Views を使って、中古車販売チェーンの本部向け「中古車販売 経営ダッシュボード」を作ってください。

■ Metric Views(集計指標 — KPI に優先利用、すべて {MY_CATALOG}.{MY_SCHEMA} 配下):
- mv_sales_kpi: 成約 KPI(契約件数・売上合計・粗利合計・粗利率・平均販売価格)
- mv_inventory_kpi: 在庫 KPI(在庫台数・平均販売予定価格・平均仕入価格・平均年式・平均走行距離)
- mv_inquiry_kpi: 引合 KPI(引合件数・成約件数・成約率)

■ Silver / Gold テーブル(明細・セグメント、すべて {MY_CATALOG}.{MY_SCHEMA} 配下):
- sl_stores: 店舗マスタ(店舗 ID・店名・都道府県・店舗フォーマット)
- sl_vehicles: 在庫車両マスタ(車両 ID・メーカー・車種・ボディタイプ・年式・走行距離・修復歴・認定中古車フラグ・販売価格)
- sl_contracts: 成約明細(契約 ID・店舗・車両・顧客・契約日・販売価格・粗利・粗利率)
- sl_market_index: 中古車相場指数(日付・車種カテゴリ・相場指数・燃料価格・為替)
- gd_customer_rm_segment: 顧客 RM セグメント(VIP / Loyal / At Risk / Lost / Prospect)

■ ダッシュボードの構成(4 ページ):
【ページ 1: 経営概況】
1. 上段 KPI カウンター × 4: 契約件数 / 売上合計 / 粗利率 / 在庫回転日数 (前月比 % 併記)
2. 月別の契約件数・売上合計トレンド(折れ線グラフ)
3. 店舗別の契約件数 / 粗利率 TOP10(横棒グラフ)
4. ボディタイプ別(軽 / コンパクト / セダン / SUV / ミニバン / 輸入車)の売上構成(円グラフ)

【ページ 2: 在庫分析】
5. 在庫台数の都道府県別ヒートマップまたは横棒
6. ボディタイプ × 年式 の在庫構成(積み上げ棒)
7. 修復歴あり / なし の販売価格分布比較(箱ひげ図またはヒストグラム)
8. 認定中古車フラグ別の粗利率比較(棒グラフ)

【ページ 3: 引合・顧客分析】
9. 引合チャネル別(来店 / オンライン / 電話 / 買取査定)の引合件数・成約率
10. AI 分類された引合カテゴリ別(購入検討 / 値引交渉 / 買取査定 / 在庫確認)の成約率
11. RM セグメント別の顧客数分布(棒グラフ)
12. RM セグメント別の平均取引額(棒グラフ)

【ページ 4: 相場連動】
13. 中古車相場指数の月次トレンド(折れ線)
14. 相場指数 vs 自社販売価格の相関(散布図)
15. 月次の燃料価格 vs ボディタイプ別売上(複合グラフ)

■ Global フィルタ(全ページ共通):
- 期間(過去 12 ヶ月のデフォルト)
- 店舗(複数選択可)
- ボディタイプ(複数選択可)

■ 表示ルール:
- 金額は万円単位で表示(ROUND(value / 10000, 1))
- 粗利率はパーセント表示(小数点 1 桁)
- 走行距離は km 単位
- ボディタイプは「軽」「コンパクト」「セダン」「SUV」「ミニバン」「輸入車」の 6 種類
- ダッシュボードのタイトルは「中古車販売 経営ダッシュボード」
- 集計指標(契約件数・売上合計・粗利率 等)は Metric Views(mv_*) を優先利用
"""

print(prompt)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 プロンプトのポイント</strong>
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li><b>Metric Views を最優先指定</b>: KPI のブレが大幅に減る(粗利率の計算式が一意に定まる)</li>
# MAGIC   <li><b>テーブル名をフルパスで指定</b>: AI がデータソースを正確に参照できる</li>
# MAGIC   <li><b>各テーブルの内容を簡潔に説明</b>: カラムの意味を AI が理解しやすくなる</li>
# MAGIC   <li><b>チャートの種類を指定</b>: 「散布図」「折れ線」「箱ひげ図」など具体的に書くと精度が上がる</li>
# MAGIC   <li><b>表示ルールを明示</b>: 万円単位・% 単位など、ビジネス要件をプロンプトに含める</li>
# MAGIC   <li><b>ページ構成を明示</b>: 4 ページ構成にすると情報整理がしやすくなる</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 3. 生成されたダッシュボードの確認
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">3. 生成されたダッシュボードの確認</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 確認ポイント</strong><br>
# MAGIC AI が生成したダッシュボードを以下の観点で確認してください:
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li>データセットの SQL: テーブル結合やフィルタは正しいか?</li>
# MAGIC   <li>Metric Views を使っているか: 集計指標の SQL に <code>MEASURE()</code> が出ているか</li>
# MAGIC   <li>チャートの種類: データの特性に合ったチャートが選ばれているか?</li>
# MAGIC   <li>レイアウト: 情報の優先度に沿った配置になっているか?</li>
# MAGIC   <li>金額の単位: 万円単位で表示されているか?</li>
# MAGIC   <li>修復歴・認定中古車・ボディタイプ等の中古車業界特有の項目が正しく表示されているか?</li>
# MAGIC </ul>
# MAGIC <br>
# MAGIC 修正したい場合は、Genie Code に追加の指示を出すか、手動で編集できます。<br>
# MAGIC 例: 「期間フィルタを追加して」「ボディタイプ別の散布図を追加して」「金額表示を万円単位に統一して」
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 うまくいかないときは</strong>
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li>AI が生成した <b>データセットの SQL を確認</b> して、テーブル名が正しいか見てみましょう</li>
# MAGIC   <li>チャートの種類が意図と違う場合は「この chart を棒グラフに変えて」と指示できます</li>
# MAGIC   <li>一度に全部作らず、「まず売上トレンドだけ作って」と <b>段階的に依頼</b> するのも有効です</li>
# MAGIC   <li>手動でデータセットを追加し、チャートを配置することもできます</li>
# MAGIC   <li>集計値がブレる場合は「<code>mv_sales_kpi</code> を必ず使って」とプロンプトに追記</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 4. ダッシュボードに Genie を連携する
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">4. ダッシュボードに Genie を連携する</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 ダッシュボード × Genie 連携とは</strong><br>
# MAGIC ダッシュボードに Genie スペースを紐づけると、閲覧者がダッシュボード上で <b>自然言語で追加の質問</b> ができるようになります。<br>
# MAGIC 「このグラフの内訳は?」「先月との比較は?」「修復歴ありの内訳は?」など、チャートを見ながら深掘りできます。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: Genie スペースをダッシュボードにリンクする</strong><br><br>
# MAGIC <b>Step 1:</b> 前のノートブック(07)で作成した <b>Genie スペース「中古車販売 E2E 分析」</b> を開きます。<br>
# MAGIC <b>Step 2:</b> Genie スペースの右上「<b>共有</b>」をクリックし、左下の「<b>リンクをコピー</b>」で URL を取得します。<br>
# MAGIC <b>Step 3:</b> ダッシュボードの編集画面に戻り、以下の手順で Genie をリンクします。
# MAGIC <ol style="margin: 6px 0 0 0;">
# MAGIC   <li>右上の「<b>⋮</b>」メニュー → 「<b>設定とテーマ</b>」をクリック</li>
# MAGIC   <li>「<b>Genie を有効にしてください</b>」のトグルを <b>ON</b></li>
# MAGIC   <li>「<b>既存の Genie space をリンク</b>」を選択</li>
# MAGIC   <li>コピーした URL から <b>Genie スペース ID</b> を貼り付け</li>
# MAGIC </ol>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 完成イメージ</strong><br>
# MAGIC Genie 連携後、ダッシュボードを公開すると閲覧者が自然言語で質問できるようになります。<br>
# MAGIC ダッシュボード右下の Genie アイコンから「先月の VIP 顧客の売上は?」と聞けば即座に SQL が生成されて回答が返ります。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ ダッシュボード作成完了</strong><br>
# MAGIC Genie Code にプロンプトを渡すだけで、データの特性に合ったダッシュボードが自動生成されることを体験しました。<br>
# MAGIC <b>テーブルコメント・PK/FK 制約・Metric Views</b> が設定済みであるほど、AI の生成精度が向上します。<br>
# MAGIC さらに Genie を連携することで、閲覧者が自然言語でダッシュボードを深掘りできるようになります。<br><br>
# MAGIC 次は <code>09_Jobsワークフロー作成手順.py</code> で、ここまでの ETL を <b>Jobs(ワークフロー)で自動化</b> します。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ トラブルシューティング</strong><br>
# MAGIC <b>Genie Code が応答しない</b>: SQL ウェアハウスが起動しているか確認 / 一度ダッシュボードを再読込<br>
# MAGIC <b>生成されたチャートが空</b>: 生成された SQL の WHERE 句や JOIN を確認 / テーブル名が完全修飾か確認<br>
# MAGIC <b>金額が円のまま表示</b>: 「全ての金額表示を万円単位に変更して」と Genie Code に追加指示<br>
# MAGIC <b>Metric Views が使われていない</b>: 「<code>mv_sales_kpi</code> 等の Metric Views を必ず使って」と追加指示<br>
# MAGIC <b>4 ページの分割がされない</b>: 「ページを 4 つに分けて、各ページのタイトルを明示して」と再依頼<br>
# MAGIC <b>フィルタが効かない</b>: ダッシュボード上部の「Add a filter」で Global Filter を手動追加 / データセット側のパラメータ参照を確認
# MAGIC </div>
