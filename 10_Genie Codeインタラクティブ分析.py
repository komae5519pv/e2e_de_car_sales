# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 10 | Genie でインタラクティブ分析
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">経営判断シナリオを Genie に投げて深掘りする — 自然言語 BI のフルポテンシャルを体験</p>
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
# MAGIC 07 で作った Genie スペースに対して、<b>経営判断シナリオ 3 本</b>を投げて深掘り分析を体験します。<br>
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li>各シナリオは <b>複数の質問を連続で投げる</b>(エージェントモード活用)</li>
# MAGIC   <li>Genie が複数テーブルを跨いで回答し、<b>そのまま意思決定に使える深さ</b>になることを確認</li>
# MAGIC   <li>下のセルから <b>プロンプトをコピー</b>して Genie スペースの入力欄に貼るだけ</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📋 前提条件</strong><br>
# MAGIC ✅ <code>07_Genie作成手順.py</code> 完了(<b>中古車販売 E2E 分析</b> Genie スペース作成済み)<br>
# MAGIC ✅ <code>08_ダッシュボード作成手順.py</code> 完了(任意 — ダッシュボードから Genie に飛ぶ体験を含める場合)<br>
# MAGIC ✅ サーバーレス SQL ウェアハウスが起動中<br>
# MAGIC ✅ Genie スペースで <b>エージェントモード</b>のトグルが ON にできる状態
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,共通設定の読み込み
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 1. シナリオ A — 「先月、利益が想定より低かった」を深掘り
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">1. シナリオ A — 「先月、利益が想定より低かった」を深掘り</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🎬 想定シーン</strong><br>
# MAGIC 月次会議で「先月の粗利率が前月より落ちている」と社長から指摘。<br>
# MAGIC 原因を突き止めて、来月の打ち手を提案するまでを Genie で 3 ステップで完結させる。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: シナリオ A — 3 つの質問を順番に投げる</strong><br><br>
# MAGIC Genie スペースを開いて、<b>エージェントモードを ON</b> にしてから、下の 3 つの質問を <b>同じ会話セッション内で連続</b>して投げてください。<br>
# MAGIC Genie が前の回答を踏まえて深掘りする様子を確認します。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,シナリオ A: 質問 1 — 全体俯瞰
prompt_a1 = """先月と前月の粗利率を全社で比較して、差分を可視化してください。
あわせて、ボディタイプ別・店舗別の粗利率変化も見せてください。"""
print(prompt_a1)

# COMMAND ----------

# DBTITLE 1,シナリオ A: 質問 2 — 原因分析(深掘り)
prompt_a2 = """粗利率が下がった店舗トップ 3 について、以下を分析してください:
- 仕入価格と販売価格の変化
- 取り扱い車種ミックス(ボディタイプ別の構成比)の変化
- 修復歴あり車両の比率の変化
どれが粗利率低下の主要因か、結論を一言添えてください。"""
print(prompt_a2)

# COMMAND ----------

# DBTITLE 1,シナリオ A: 質問 3 — 打ち手提案
prompt_a3 = """粗利率が下がった原因を踏まえて、来月、当該店舗が取るべき打ち手を 3 つ提案してください。
データに基づく根拠を必ず添えてください。"""
print(prompt_a3)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 シナリオ A のポイント</strong>
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li><b>質問 1</b>: Metric Views(<code>mv_sales_kpi</code>)を中心に <b>全社→セグメント別</b>のドリルダウン</li>
# MAGIC   <li><b>質問 2</b>: 複数テーブル(<code>sl_contracts</code> + <code>sl_vehicles</code>)を跨いだ <b>原因分析</b>(エージェントモード)</li>
# MAGIC   <li><b>質問 3</b>: データに基づく <b>提案フェーズ</b> — Genie が分析結果を要約して打ち手を出す</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 2. シナリオ B — VIP 顧客の「離反兆候」を発見する
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">2. シナリオ B — VIP 顧客の「離反兆候」を発見する</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🎬 想定シーン</strong><br>
# MAGIC 営業部長から「最近、リピート客の動きが鈍い気がする」との相談。<br>
# MAGIC RM セグメントを使って、離反リスクの高い VIP 顧客を特定し、アプローチリストを作成する。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,シナリオ B: 質問 1 — 全体トレンド
prompt_b1 = """RM セグメント(VIP / Loyal / At Risk / Lost / Prospect)別に、
現在の顧客数・平均取引額・最終接点からの経過月数を一覧で見せてください。"""
print(prompt_b1)

# COMMAND ----------

# DBTITLE 1,シナリオ B: 質問 2 — 離反兆候の特定
prompt_b2 = """At Risk セグメントの顧客のうち、過去に VIP / Loyal だった顧客を抽出してください。
最後に契約した車のボディタイプ・販売価格・契約日を一緒に表示してください。
リストは最終接点が古い順で。"""
print(prompt_b2)

# COMMAND ----------

# DBTITLE 1,シナリオ B: 質問 3 — アプローチ提案
prompt_b3 = """At Risk に落ちた元 VIP / Loyal 顧客に対して、
最後に買った車のボディタイプを踏まえた <b>乗り換え提案</b>(現在の在庫車両から候補)を提示できるか、
店舗別に在庫マッチング状況を見せてください。"""
print(prompt_b3)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 シナリオ B のポイント</strong>
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li><b>質問 1</b>: <code>gd_customer_rm_segment</code> を中心にしたシンプルな集計</li>
# MAGIC   <li><b>質問 2</b>: <b>セグメント遷移</b>(VIP → At Risk)を歴史データから抽出する複雑な分析</li>
# MAGIC   <li><b>質問 3</b>: 顧客 × 在庫マッチング — エージェントモードでないと難しい横断クエリ</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 3. シナリオ C — 「相場 × 季節性 × 商談チャネル」で来期戦略を立てる
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">3. シナリオ C — 「相場 × 季節性 × 商談チャネル」で来期戦略を立てる</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🎬 想定シーン</strong><br>
# MAGIC 経営企画から「来期の販売戦略立案のため、外部要因(相場・燃料価格)と内部要因(チャネル・商談カテゴリ)の関係を整理してほしい」と依頼。<br>
# MAGIC 04 で AI 分類した <code>sl_inquiries_enriched</code> を活用して、データドリブンな戦略案を作る。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,シナリオ C: 質問 1 — 外部要因の影響
prompt_c1 = """過去 12 ヶ月の中古車相場指数と、当社の月次売上・成約率の相関を分析してください。
相場が下落した月に、当社の販売動向がどう動いたか、グラフで見せてください。"""
print(prompt_c1)

# COMMAND ----------

# DBTITLE 1,シナリオ C: 質問 2 — チャネル × カテゴリ別の効率
prompt_c2 = """商談チャネル(来店 / オンライン / 電話 / 買取査定)× AI 分類された引合カテゴリ(購入検討 / 値引交渉 / 買取査定 / 在庫確認 / その他)別に、
引合件数と成約率のクロス集計を見せてください。
最も成約率が高い組み合わせトップ 3 はどれですか?"""
print(prompt_c2)

# COMMAND ----------

# DBTITLE 1,シナリオ C: 質問 3 — 来期戦略の提案
prompt_c3 = """ここまでの分析を踏まえて、来期(次の 6 ヶ月)に注力すべき:
- ボディタイプ × 月(季節性)
- 商談チャネル × 引合カテゴリ
の組み合わせを 3 つ提案してください。
相場指数の予測と、チャネル別の成約率を根拠に説明してください。"""
print(prompt_c3)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 シナリオ C のポイント</strong>
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li><b>質問 1</b>: 相場 × 売上の <b>外部要因連動分析</b>(<code>sl_market_index</code> + <code>mv_sales_kpi</code>)</li>
# MAGIC   <li><b>質問 2</b>: 04 の <b>AI 分類カテゴリ</b>を使ったクロス集計 — Bronze/Silver の生データだけではできない切り口</li>
# MAGIC   <li><b>質問 3</b>: 複数視点を <b>統合した戦略提案</b> — Genie が経営企画の壁打ち相手になる</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 4. ダッシュボードから Genie を起動する
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">4. ダッシュボードから Genie を起動する</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #388E3C; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 ハンズオン: ダッシュボード閲覧者の体験を再現</strong><br><br>
# MAGIC 08 で作ったダッシュボードを開き、Genie 連携済み(08 の Step 4 を実施済み)の状態で、<br>
# MAGIC 右下の <b>Genie アイコン</b>から自然言語で深掘りしてみましょう。<br><br>
# MAGIC <strong>試す質問の例:</strong>
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li>「この KPI(粗利率)を都道府県別に分解して」</li>
# MAGIC   <li>「在庫回転日数が長い店舗トップ 5 と、その車種構成を教えて」</li>
# MAGIC   <li>「先月、認定中古車と非認定中古車で粗利率にどれくらい差があった?」</li>
# MAGIC </ul>
# MAGIC <br>
# MAGIC ➡ ダッシュボードを <b>静的なレポート</b>から <b>対話型の分析環境</b>に変える効果を体感できます。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 ダッシュボード × Genie のメリット</strong>
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li><b>閲覧者がチャートを見ながらその場で深掘り</b>できる(SQL不要)</li>
# MAGIC   <li>気になった数字をクリック → そのコンテキストで質問</li>
# MAGIC   <li>分析担当者が定型レポートを毎回書く負荷がなくなる</li>
# MAGIC   <li>「あれ調べて」「これ調べて」がチャットで完結 → 意思決定スピード向上</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 5. Genie の精度を上げ続けるために
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">5. Genie の精度を上げ続けるために</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 運用フェーズで効くチューニング</strong>
# MAGIC <table style="width: 100%; border-collapse: collapse; border-radius: 8px; overflow: hidden; margin-top: 8px;">
# MAGIC   <thead>
# MAGIC     <tr style="background: #1976D2; color: white;">
# MAGIC       <th style="padding: 10px 15px; text-align: left;">施策</th>
# MAGIC       <th style="padding: 10px 15px; text-align: left;">効果</th>
# MAGIC       <th style="padding: 10px 15px; text-align: left;">頻度</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background: #f9f9f9;">
# MAGIC       <td style="padding: 8px 15px;"><b>👍 / 👎 フィードバック</b></td>
# MAGIC       <td style="padding: 8px 15px;">Genie の学習データに反映 — 精度が継続的に向上</td>
# MAGIC       <td style="padding: 8px 15px;">毎回</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 8px 15px;"><b>サンプル質問の追加</b></td>
# MAGIC       <td style="padding: 8px 15px;">よく聞かれる質問をテンプレ化 — 似た質問の精度向上</td>
# MAGIC       <td style="padding: 8px 15px;">月次レビュー</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #f9f9f9;">
# MAGIC       <td style="padding: 8px 15px;"><b>General Instructions の更新</b></td>
# MAGIC       <td style="padding: 8px 15px;">業務用語・回答ルールの精緻化</td>
# MAGIC       <td style="padding: 8px 15px;">四半期</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 8px 15px;"><b>テーブル / カラムコメント追加</b></td>
# MAGIC       <td style="padding: 8px 15px;">新しいカラムが追加されたとき、意味を Genie に伝える</td>
# MAGIC       <td style="padding: 8px 15px;">スキーマ変更時</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #f9f9f9;">
# MAGIC       <td style="padding: 8px 15px;"><b>Metric Views の拡充</b></td>
# MAGIC       <td style="padding: 8px 15px;">業務指標を増やす — Genie が同じ指標で常に同じ答えを返す</td>
# MAGIC       <td style="padding: 8px 15px;">新指標が必要なとき</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ E2E ハンズオン完走、おつかれさまでした!</strong><br>
# MAGIC <b>00 → 10</b> までで、Databricks 上に
# MAGIC <ul style="margin: 6px 0 0 0;">
# MAGIC   <li>Unity Catalog でガバナンスされたメダリオンアーキテクチャ(Bronze / Silver / Gold)</li>
# MAGIC   <li>SDP で宣言的に定義した ETL パイプライン</li>
# MAGIC   <li>音声 → Whisper / AI Functions による非構造データ処理</li>
# MAGIC   <li>UC Metric Views による業務指標の一元管理</li>
# MAGIC   <li>Genie / AI/BI ダッシュボードによるノーコード分析</li>
# MAGIC   <li>Jobs による End-to-End 自動化</li>
# MAGIC </ul>
# MAGIC を構築し、<b>経営判断シナリオを Genie で深掘り</b>するところまで体験しました。<br><br>
# MAGIC ここまでの一連の流れが <b>Databricks Data Intelligence Platform の真価</b>です。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>⚠️ トラブルシューティング</strong><br>
# MAGIC <b>Genie がシナリオ B / C で「テーブルが結合できない」と返す</b>: 05 で PK / FK 制約が設定されているか確認<br>
# MAGIC <b>エージェントモードの応答が遅い</b>: 複数 SQL を順次実行するため、通常モードより 30 秒〜 1 分長くなることがあります(正常)<br>
# MAGIC <b>「相場 × 売上の相関」がうまく出ない</b>: <code>sl_market_index</code> と <code>mv_sales_kpi</code> を両方登録しているか確認<br>
# MAGIC <b>「VIP → At Risk のセグメント遷移」が取れない</b>: <code>gd_customer_rm_segment</code> に履歴情報がない場合は、現在のセグメントのみで分析するよう Genie に指示<br>
# MAGIC <b>金額が円のまま表示される</b>: General Instructions で「万円単位」を明記済みか確認(07 で設定済み)
# MAGIC </div>
