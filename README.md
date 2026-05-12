# 中古車販売 Databricks E2E デモ

## 概要

中古車販売を題材に、Databricks Data Intelligence Platform 上で **データ取り込み → 加工 → ガバナンス → 分析 → AI 活用** までを一気通貫で体験するデモ教材。
構造化データのパイプライン（SDP + Autoloader + Expectations）、Unity Catalog によるガバナンス、UC Metrics Views によるセマンティクス層、PDF カタログを題材とした RAG（`ai_parse_document` + `ai_prep_search` + Vector Search）、AI/BI Dashboard・Genie・Databricks One によるアウトプット体験までを通して、プラットフォームの機能を確認できる構成です。

## 対象 / 形式

| 項目 | 内容 |
|---|---|
| 対象 | データ基盤・分析基盤の評価担当者 |
| 形式 | デモ（手を動かすハンズオンは別企画） |
| 所要時間 | 約 2 時間 |
| 環境 | Databricks サーバレスワークスペース |

## 想定利用者と活用ツール

中古車販売事業の各ロールが、どのツールでどんな課題を解くかを想定したシナリオ構成です。

| 想定利用者 | 主な関心事 | メインツール | サブツール |
|---|---|---|---|
| 経営企画 / 役員 | 月次粗利・店舗ランキング・エリア別業績 | AI/BI Dashboard | Genie |
| エリアマネージャー / 店舗運営 | 商談ファネル・業績悪化店検知 | AI/BI Dashboard（店舗フィルター） | Genie |
| バイヤー（仕入担当） | 相場 vs 販売粗利・滞留在庫・修復歴別収益 | Genie | AI/BI Dashboard |
| 店長 / 営業担当（現場） | 商談中の車両仕様確認・顧客への提案 | RAG / マルチエージェント（Databricks One 経由） | — |
| マーケ / CRM 担当 | 顧客セグメント・休眠掘り起こし・LTV | Genie | AI/BI Dashboard |

## デモ構成（10 ステップ）

| # | ステップ | 形式 | 紹介機能 | 時間 |
|---|---|---|---|---|
| 1 | 導入 | スライド | プラットフォーム概要・本日の流れ | 5 分 |
| 2 | データパイプライン | UI + NB | Spark Declarative Pipelines（SDP）+ Autoloader + Expectations | 25 分 |
| 3 | NB 補完処理 | NB（SQL + Python） | SDP では書きにくい複雑処理、AI Functions の利用 | 15 分 |
| 4 | オーケストレーション | UI | Jobs（SDP パイプライン + NB の混在 DAG） | 5 分 |
| 5 | UC ガバナンス | UI（一部 NB） | コメント・PK/FK・リネージ・インサイト・タグ・PII マスキング | 15 分 |
| 6 | UC Metrics Views | NB + UI | 共通 KPI のセマンティクス層（YAML 定義） | 10 分 |
| 7 | RAG 構築 | NB + UI | `ai_parse_document` + `ai_prep_search` + Vector Search Index | 15 分 |
| 8 | マルチエージェント | UI（Playground） | RAG エージェント + Genie のマルチエージェント | 10 分 |
| 9 | AI/BI Dashboard + Genie | UI | Materialized View による差分更新ダッシュボード + 自然言語クエリ | 15 分 |
| 10 | Databricks One | UI | 業務担当者向けポータル | 5 分 |

合計 120 分

## 技術スタック

| カテゴリ | 技術 |
|---|---|
| コンピュート | Serverless（SQL Warehouse / Notebook Serverless / SDP Serverless） |
| 言語 | SQL, Python (PySpark) |
| 取り込み | Autoloader（`cloud_files`）+ Volume |
| パイプライン | Spark Declarative Pipelines（SDP）+ Expectations |
| 加工 | SDP（宣言的）+ Notebook（SQL + Python 混在） |
| テーブルフォーマット | Delta Lake |
| ガバナンス | Unity Catalog（カタログ / スキーマ / Volume / コメント / PK・FK / リネージ / インサイト / タグ / カラムマスキング） |
| セマンティクス | UC Metrics Views（YAML 定義） |
| 高速化 | Materialized View + 差分更新 |
| AI Functions | `ai_classify`, `ai_query`, `ai_parse_document`, `ai_prep_search` |
| RAG | Vector Search Index |
| エージェント | Playground マルチエージェント |
| 可視化 | AI/BI Dashboard, Genie Space, Genie Code |
| エンドユーザー | Databricks One |
| ワークフロー | Jobs（SDP パイプライン + Notebook 混在 DAG） |

## メダリオンアーキテクチャ

```
[Volume: raw_data]
       │  日次 CSV / PDF カタログを順次配置
       ▼
[Autoloader (cloud_files)]
       │  新着ファイルのみ増分取り込み
       ▼
[Bronze: bz_*]   ← Raw 形式保持・監査列付与（_ingested_at / _source_file）
       │  SDP + Expectations（型不正 / NULL / 重複の品質チェック）
       ▼
[Silver: sl_*]   ← クレンジング・JOIN・正規化済み
       │  SDP（標準集計）+ Notebook（複雑処理 / AI Functions で顧客プロファイル enrich）
       ▼
[Gold: gd_*]     ← ビジネスマート（店舗 × 月、顧客セグメント、相場連動 等）
       │
       ├─►[MV: mv_*]            ← Dashboard 専用、差分更新
       │
       └─►[Metrics Views: metric_*]  ← Genie / Dashboard / SQL から共通呼出


[Volume: PDF カタログ]
       │  ai_parse_document（PDF 構造化）
       ▼
[Bronze: bz_catalog_parsed]
       │  ai_prep_search（チャンク + メタデータ自動生成）
       ▼
[Silver: sl_catalog_chunks]
       │  Vector Search Index
       ▼
[VS Index: idx_catalog]
       │
       └─►[Playground マルチエージェント]
            ├─ RAG エージェント（VS Index 参照）
            └─ Genie エージェント（販売・在庫データ参照）
```

## サンプルデータ

### 構造化データ（中古車販売）

| データ | 件数 | 主要カラム |
|---|---|---|
| 店舗マスタ（stores） | 30 | 店舗 ID, 店名, 業態（中古車プラザ / 買取専門 / 輸入車専門）, 都道府県, 緯度経度 |
| 車両在庫（vehicles） | 1,500 | 車両 ID, メーカー, 車種, ボディタイプ※, 年式, 走行距離, 修復歴, 認定中古車, VIN, ナンバー, グレード, 仕入価格, 販売価格, 入庫日, ステータス（在庫中 / 商談中 / 成約済 / 出庫済） |
| 顧客マスタ（customers） | 2,500 | 顧客 ID, 氏名, 年齢, 性別, 電話番号, 居住エリア, 登録店舗, 初回接点日, 年収レンジ, 家族構成, ライフステージ, 希望ボディタイプ, 予算上限 |
| 商談履歴（inquiries） | 12,500 | 商談 ID, 顧客, 店舗, 車両, 商談日, 来店区分（来店 / オンライン / 買取査定 / 電話）, ステータス, 商談メモ |
| 成約・粗利（contracts） | 5,000 | 契約 ID, 商談, 顧客, 店舗, 車両, 契約日, 販売価格, 仕入価格, 粗利, 粗利率, 決済方法 |
| 中古車相場指数（market_index） | 730 | 日付, 車種カテゴリ※, 相場指数, レギュラー価格, 為替 USD/JPY |

※ `vehicles.ボディタイプ` と `market_index.車種カテゴリ` は同一語彙で統一：`軽自動車 / コンパクト / セダン / SUV / ミニバン / 輸入車`

PII マスキング対象：氏名・電話番号・VIN・ナンバープレート（`mask_name` / `mask_phone` / `mask_vin` / `mask_plate`）

Autoloader デモ用にトランザクション系（inquiries / contracts）は **5 日分の日次 CSV に分割** して Volume に配置し、新着ファイルが増分取り込みされる挙動を確認できるようにしています。

`customers` の年収レンジ・家族構成・ライフステージ・希望ボディタイプ・予算上限の一部は、`inquiries.商談メモ` から `ai_query` で抽出する構成にしており、AI Functions を用いたプロファイル enrich の例として確認できます。

### 非構造化データ（RAG 用）

| データ | 件数 | 内容 |
|---|---|---|
| 車両カタログ PDF | 5（ハリアー / ヴェゼル / シエンタ / ノート / N-BOX） | 各車種 3〜4 グレード分の諸元・WLTC 燃費・グレード別装備比較・価格帯（架空カタログ） |

PDF カタログを `ai_parse_document` で構造化 → `ai_prep_search` でチャンク + メタデータ生成 → Vector Search Index 作成 → RAG エージェントから参照、という流れを通して確認できます。

## カタログ・スキーマ規約

| 項目 | 値 |
|---|---|
| カタログ | `komae_demo_v4`（事前作成済み前提） |
| スキーマ | `used_car_e2e_demo`（このデモ専用） |
| Volume | `raw_data`（CSV / PDF 共通配置） |
| Volume パス | `/Volumes/komae_demo_v4/used_car_e2e_demo/raw_data` |

テーブル命名規則：

| プレフィックス | 層 | 例 |
|---|---|---|
| `bz_*` | Bronze（Raw 取り込み） | `bz_vehicles`, `bz_contracts`, `bz_catalog_parsed` |
| `sl_*` | Silver（クレンジング・JOIN） | `sl_vehicles`, `sl_catalog_chunks` |
| `gd_*` | Gold（ビジネスマート） | `gd_store_sales_summary`, `gd_customer_segment` |
| `mv_*` | Materialized View（Dashboard 用） | `mv_dashboard_kpi`, `mv_monthly_sales` |
| `metric_*` | UC Metrics View（セマンティクス層） | `metric_sales_summary`, `metric_customer` |
| `idx_*` | Vector Search Index | `idx_catalog` |

## ファイル構成

```
E2E_DataEngineerting/
├── 00_config.py                              共通: 変数定義・スキーマ・Volume 作成
├── 01_データ準備.py                          事前実行: 構造化 CSV（日次分割）+ PDF カタログ + 商談録音 MP3 を Volume 配置
├── 02_SDPパイプライン定義.sql                SDP コード本体: Bronze/Silver/Gold + Expectations（SQL 単体ファイル）
├── 03_SDPパイプライン設定手順.py             UI 操作手順: パイプライン作成・Trigger・実行・スキーマ設定
├── 04_AIによる非構造データ処理.py            音声 → Whisper 文字起こし / ai_classify / ai_extract / ai_query / ai_parse_document
├── 05_テーブル設定.py                        コメント / PK・FK / カラムマスキング / タグ
├── 06_UC_Metrics_Views.py                    Metrics Views 定義（中古車 KPI セマンティクス）
├── 07_Genie作成手順.py                       Genie Space 作成 + General Instructions + サンプル質問（通常 × 3 + エージェント × 3）
├── 08_ダッシュボード作成手順.py              AI/BI Dashboard を Genie Code（プロンプト）で自動生成 + Genie 連携
├── 09_Jobsワークフロー作成手順.py            Jobs（SDP + Notebook 混在 DAG）+ スケジュール + 通知
├── 10_Genie Codeインタラクティブ分析.py      経営判断シナリオ 3 本(粗利低下分析 / VIP 離反検知 / 来期戦略)を Genie で深掘り
└── README.md
```

## 実行順序

1. **00_config.py** を最初に実行（スキーマ・Volume 作成）
2. **01_データ準備.py** を実行（CSV / PDF / 音声 MP3 を Volume に配置）
3. **02_SDPパイプライン定義.sql** は SDP として登録するための定義ファイル。実体の実行は `03_SDPパイプライン設定手順.py` の UI 操作で行う
4. **04_AIによる非構造データ処理.py** で音声・テキストの AI enrich(Whisper / ai_classify / ai_extract / ai_query / ai_parse_document)
5. **05** で UC ガバナンス、**06** で Metrics Views、**07** で Genie、**08** でダッシュボード、**09** で Jobs ワークフロー、**10** で Genie Code を使った経営判断シナリオ深掘り体験
6. `07`〜`10` の UI 操作系は手順 NB に従って Databricks UI で操作する

## 各ステップで確認できるポイント

### 2. データパイプライン（SDP + Autoloader + Expectations）
- `cloud_files()` による新着ファイルのみの増分取り込み
- `@dlt.expect_or_drop` / `@dlt.expect_or_fail` によるデータ品質ルール
  - 例：販売価格 > 仕入価格 / 年式 ≥ 1990 / VIN は 17 桁
- Expectation 違反レコード件数のメトリクス自動収集
- パイプライン UI でのデータ系統（Lineage）自動描画

### 3. NB 補完処理 + AI Functions
- SDP では書きにくい処理を Notebook で SQL + Python ハイブリッドで実装
- `ai_query` を用いて `inquiries.商談メモ` から顧客プロファイル（年収レンジ・家族構成・ライフステージ等）を抽出し、`customers` を enrich
- `ai_classify` による商談メモのトピック分類

### 5. UC ガバナンス
| 機能 | 確認ポイント |
|---|---|
| テーブル / カラムコメント | 辞書ベースで一括設定、カタログ UI 上の自然言語検索 |
| PK / FK 制約 | 制約から ER 図が自動生成 |
| リネージ | `bz_*` → `sl_*` → `gd_*` → `mv_*` の系統がクリックで追える |
| インサイト | ① このテーブルを参照する Dashboard / NB の自動表示 ② 誰がどんなクエリを実行したか ③ ヘビーユーザー TOP |
| カラムマスキング | 氏名・電話・VIN・ナンバープレートの動的マスキング |

### 6. UC Metrics Views
- 中古車向け KPI（販売台数・売上・粗利・粗利率・在庫日数）を YAML で定義
- Genie / Dashboard / SQL から **同じ定義** で参照
- FILTER 句・Composable measure の組み込み

### 7. RAG 構築
- PDF を SQL 関数 `ai_parse_document` で構造化（VARIANT 型）
- `ai_prep_search` でチャンク（`chunk_id` / `chunk_to_embed`）+ メタデータを自動生成
- Vector Search Index は UI から Source Table・PK・Embedding カラムを選択して作成

### 8. マルチエージェント
- Playground 上で RAG エージェント（PDF カタログ参照）+ Genie エージェント（販売・在庫データ参照）を連携
- 1 つの問い合わせから両エージェントが連携して回答する流れを確認

### 9. AI/BI Dashboard + Genie
- Gold 層から Dashboard 専用の Materialized View（`mv_*`）を作成
- `REFRESH MATERIALIZED VIEW` の差分更新挙動
- ダッシュボードから Genie への自然言語クエリ連携

### 10. Jobs（SDP + NB 混在 DAG）
```
[Task1: SDP パイプライン実行]
        ▼
[Task2: NB 補完処理（AI Functions）]
        ▼
[Task3: NB Metrics Views 更新]
        ▼
[Task4: NB MV 差分更新]
```

## 入力プロンプト集（再現用）

各機能で使う入力プロンプトの一覧です。詳細・追加例は対応する NB（06 / 08 / 09）に記載しています。

### Genie Space サンプル質問（ステップ 9）
通常質問：
- 「先月、最も粗利が高かった店舗は？」
- 「軽自動車の在庫回転日数は？」
- 「20 代女性に売れている車種 TOP 3 は？」

エージェントモード：
- 「相場指数が下落した月の販売動向は？」
- 「修復歴ありの粗利率を店舗別に比較して」
- 「商圏人口と売上の相関を見せて」

### マルチエージェント デモ質問（ステップ 8）
- 「ハリアー Z グレードの装備と、現在の在庫状況・販売実績を教えて」（RAG + Genie 両参照）
- 「ヴェゼル e:HEV の WLTC 燃費は？ 似た燃費性能の在庫車両は？」（RAG → Genie）
- 「先月最も成約した車種について、グレード別の特徴を教えて」（Genie → RAG）

### RAG 検索クエリ例（ステップ 7）
- 「ハリアーのハイブリッド WLTC 燃費」
- 「シエンタ 7 人乗りグレードの違い」
- 「N-BOX カスタムの安全装備」

### AI/BI Dashboard プロンプト（ステップ 9 / Genie Code）
ダッシュボード作成用の自然言語プロンプト：
- 「店舗別 × 月別の販売台数と粗利率をヒートマップで」
- 「車種カテゴリ別の販売構成比をドーナツチャートで」
- 「年式 × 販売価格の散布図、ボディタイプで色分け」

詳細プロンプトとウィジェット仕様は `09_AI_BI_Dashboard手順.py` を参照。

## 技術スタック詳細（リソース管理）

PoC・引き継ぎ・運用時のリファレンスとして、本デモで作成・利用するすべての Databricks リソースを一覧化したセクションです。  
Pipeline ID / Job ID / Dashboard ID / Genie Space ID は実行後に埋めてください。

### 1. Unity Catalog リソース

| 項目 | 値 |
|---|---|
| カタログ | `komae_demo_v4`（事前作成済み） |
| スキーマ | `komae_demo_v4.used_car_e2e_demo` |
| Volume | `komae_demo_v4.used_car_e2e_demo.raw_data` |
| Volume パス | `/Volumes/komae_demo_v4/used_car_e2e_demo/raw_data` |

### 2. テーブル / ビュー一覧（24 オブジェクト）

#### Bronze（6 テーブル / SDP Streaming Table）
| テーブル | ソース | 説明 |
|---|---|---|
| `bz_stores` | `raw_data/stores.csv` | 店舗マスタ Bronze（30 件） |
| `bz_vehicles` | `raw_data/vehicles.csv` | 車両在庫 Bronze（1,500 件） |
| `bz_customers` | `raw_data/customers.csv` | 顧客マスタ Bronze（2,500 件） |
| `bz_market_index` | `raw_data/market_index.csv` | 中古車相場指数 Bronze（730 件） |
| `bz_inquiries` | `raw_data/inquiries/*.csv`（5 日分） | 商談履歴 Bronze（12,500 件） |
| `bz_contracts` | `raw_data/contracts/*.csv`（5 日分） | 成約・粗利 Bronze（5,000 件） |

#### Silver（6 テーブル / SDP Streaming Table）
| テーブル | PK | FK | 説明 |
|---|---|---|---|
| `sl_stores` | `store_id` | — | 店舗マスタ |
| `sl_vehicles` | `vehicle_id` | `store_id` → `sl_stores` | 車両在庫（Expectations: 年式 / VIN17 桁 / 価格） |
| `sl_customers` | `customer_id` | `registered_store_id` → `sl_stores` | 顧客マスタ |
| `sl_market_index` | `(date, vehicle_category)` | — | 中古車相場指数（複合 PK） |
| `sl_inquiries` | `inquiry_id` | `customer_id` / `store_id` / `vehicle_id` | 商談履歴 |
| `sl_contracts` | `contract_id` | `inquiry_id` / `customer_id` / `store_id` / `vehicle_id` | 成約・粗利 |

#### Gold MV（3 テーブル / SDP Materialized View）
| テーブル | PK | FK | 説明 |
|---|---|---|---|
| `gd_store_monthly_sales` | `(store_id, sales_month)` | `store_id` → `sl_stores` | 店舗 × 月別 販売台数・粗利 |
| `gd_vehicle_inventory` | `(body_type, status)` | — | ボディタイプ別 在庫状況サマリ |
| `gd_market_linked_margin` | `(month, vehicle_category)` | — | 相場 × 粗利率の月次比較 |

#### 04 NB enrich（5 テーブル / Delta）
| テーブル | PK | FK | 説明 |
|---|---|---|---|
| `sl_inquiries_enriched` | `inquiry_id` | `customer_id` / `store_id` / `vehicle_id` | 商談メモ AI 分類 + Whisper 文字起こし |
| `sl_customers_enriched` | `customer_id` | `customer_id` → `sl_customers` | `ai_query` で年収・家族構成を enrich |
| `sl_pdf_catalogs_parsed` | `catalog_name` | — | PDF を `ai_parse_document` で構造化 |
| `gd_contract_margin_score` | `contract_id` | `contract_id` / `store_id` / `vehicle_id` / `customer_id` | 月次×店舗×車種カテゴリの粗利スコア |
| `gd_customer_rm_segment` | `customer_id` | `customer_id` → `sl_customers` | RM 分析（最終接点経過月 × 累計取引額） |

#### Metric Views（3 / UC Metric Views）
| Metric View | 主要 Measure | 主要 Dimension |
|---|---|---|
| `metric_sales_summary` | 販売台数 / 粗利 / 粗利率 | 店舗 / 月 / 車種カテゴリ |
| `metric_inventory` | 在庫日数 / 在庫件数 / 平均価格 | ボディタイプ / 修復歴 / 認定中古車 |
| `metric_customer` | 顧客数 / 平均購入単価 / RM セグメント分布 | RM セグメント / 年代 / 性別 |

#### Vector Search Index（1）
| Index | Source Table | PK | Embedding 列 |
|---|---|---|---|
| `idx_catalog` | `sl_catalog_chunks` | `chunk_id` | `chunk_to_embed` |

### 3. PK / FK 制約サマリ

| 階層 | PK 数 | FK 数 | 設定方法 |
|---|---|---|---|
| Silver SDP | 6 | 9 | 02 の `CREATE OR REFRESH STREAMING TABLE` にインライン宣言 |
| Gold MV | 3 | 1 | 02 の `CREATE OR REFRESH MATERIALIZED VIEW` にインライン宣言 |
| 04 Delta | 5 | 9 | 05 の `ALTER TABLE ADD CONSTRAINT` で追加 |
| **合計** | **14** | **19** | |

### 4. AI Functions / モデル

| 利用機能 | 関数 | モデル | 使用ノートブック |
|---|---|---|---|
| 商談メモのトピック分類 | `ai_classify` | `databricks-claude-opus-4-7` | 04 |
| 顧客プロファイル enrich | `ai_query` | `databricks-claude-opus-4-7` | 04 |
| PDF 構造化 | `ai_parse_document` | （内部モデル） | 04 |
| RAG 用チャンク生成 | `ai_prep_search` | （内部モデル） | 07 |

### 5. Model Serving エンドポイント

| エンドポイント | 用途 | デプロイ元 |
|---|---|---|
| `komae_whisper_large_v3` | 商談録音 MP3 の文字起こし | UC 上の Whisper Large v3 モデル |

### 6. Genie Space / AI/BI Dashboard

| リソース | 名前 | ID | 作成 NB |
|---|---|---|---|
| Genie Space | 中古車販売 E2E 分析 | `_______________` | 07 |
| Dashboard | 中古車販売 E2E ダッシュボード | `_______________` | 08 |

### 7. SDP Pipeline / Jobs

| リソース | 名前 | ID | 作成 NB |
|---|---|---|---|
| SDP パイプライン | E2E_used_car_pipeline | `_______________` | 03 |
| Jobs ワークフロー | 中古車 E2E ETL | `_______________` | 09 |

### 8. PII マスキング関数（5_テーブル設定.py で定義）

| 関数名 | 適用列 | 動作 |
|---|---|---|
| `mask_name` | `sl_customers.name` | `is_account_group_member` 不一致時に伏字 |
| `mask_phone` | `sl_customers.phone` | 末尾 4 桁以外を伏字 |
| `mask_vin` | `sl_vehicles.vin` | 17 桁全て伏字 |
| `mask_plate` | `sl_vehicles.plate_number` | 全伏字 |

### 9. Volume 配置物

| パス | 内容 | 件数 |
|---|---|---|
| `/Volumes/.../raw_data/stores.csv` | 店舗マスタ CSV | 30 |
| `/Volumes/.../raw_data/vehicles.csv` | 車両在庫 CSV | 1,500 |
| `/Volumes/.../raw_data/customers.csv` | 顧客マスタ CSV | 2,500 |
| `/Volumes/.../raw_data/market_index.csv` | 中古車相場指数 CSV | 730 |
| `/Volumes/.../raw_data/inquiries/*.csv` | 商談履歴 日次分割 | 5 日分 |
| `/Volumes/.../raw_data/contracts/*.csv` | 成約・粗利 日次分割 | 5 日分 |
| `/Volumes/.../raw_data/pdf/*.pdf` | 車両カタログ PDF | 5（ハリアー / ヴェゼル / シエンタ / ノート / N-BOX） |
| `/Volumes/.../raw_data/audio/*.mp3` | 商談録音 MP3 | 数件（Whisper デモ用） |

### 10. ノートブック → リソース マッピング

| NB | 作成・更新するリソース |
|---|---|
| `00_config.py` | スキーマ / Volume |
| `01_データ準備.py` | Volume 配置物（CSV / PDF / MP3） |
| `02_SDPパイプライン定義.sql` | Bronze 6 / Silver 6 / Gold MV 3（定義のみ。実行は 03） |
| `03_SDPパイプライン設定手順.py` | SDP パイプライン本体（UI 操作） |
| `04_AIによる非構造データ処理.py` | 04 Delta 5 テーブル + ai_classify/ai_query/ai_parse_document/Whisper |
| `05_テーブル設定.py` | 全テーブルのコメント / 04 Delta の PK/FK / PII 列マスク |
| `06_UC_Metrics_Views.py` | Metric View 3 件 |
| `07_Genie作成手順.py` | Genie Space（UI 操作） |
| `08_ダッシュボード作成手順.py` | AI/BI Dashboard + Genie Code（UI 操作） |
| `09_Jobsワークフロー作成手順.py` | Jobs（UI 操作） |
| `10_Genie Codeインタラクティブ分析.py` | Genie への質問プロンプト（NB 内に出力） |
