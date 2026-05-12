# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 01 | データ準備
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">中古車販売 E2E デモ</p>
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
# MAGIC デモで利用する <b>構造化データ（CSV）</b> と <b>非構造化データ（PDF カタログ）</b> を生成し、Unity Catalog Volume に配置します。<br>
# MAGIC <ul style="margin: 8px 0 0 0;">
# MAGIC   <li>マスタ系 4 ファイル（stores / vehicles / customers / market_index）</li>
# MAGIC   <li>トランザクション系（inquiries / contracts）は <b>履歴バルク + 直近 5 日分の日次 CSV</b> に分割（Autoloader 増分取り込みデモ用）</li>
# MAGIC   <li>車両カタログ PDF 5 車種（ハリアー / ヴェゼル / シエンタ / ノート / N-BOX）</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📦 ライブラリインストール</strong><br>
# MAGIC PDF カタログ生成のため <code>reportlab</code> を利用します（日本語は IPAex Gothic を TTF 埋め込み）。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,ライブラリインストール
# MAGIC %pip install reportlab faker japanize-matplotlib --quiet

# COMMAND ----------

# DBTITLE 1,Python 再起動
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,共通設定の読み込み
# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,ライブラリインポート・シード固定
import os
import random
import datetime as dt
from pathlib import Path
import pandas as pd
import numpy as np

# 再現性のためのシード固定
SEED = 42
random.seed(SEED)
np.random.seed(SEED)

print(f"Volume path: {VOLUME_PATH}")

# COMMAND ----------

# DBTITLE 1,Volume 配下のディレクトリ構成
# 整理用にサブフォルダを切る
SUBDIRS = [
    "master",
    "transactions/inquiries",
    "transactions/contracts",
    "pdf_catalogs",
    "audio",
]

for sub in SUBDIRS:
    full = f"{VOLUME_PATH}/{sub}"
    dbutils.fs.mkdirs(full)
    print(f"✅ {full}")

# COMMAND ----------

# DBTITLE 1,既存ファイルのクリア（再実行用）
# デモを何度でもやり直せるように、既存の生成物を削除しておく
for sub in SUBDIRS:
    target = f"{VOLUME_PATH}/{sub}"
    try:
        files = dbutils.fs.ls(target)
        for f in files:
            dbutils.fs.rm(f.path, recurse=True)
        print(f"🧹 cleared {target} ({len(files)} files)")
    except Exception as e:
        print(f"(skip) {target}: {e}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 🏪 1. 店舗マスタ（stores）
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🏪 1. 店舗マスタ（stores）</strong><br>
# MAGIC 30 店舗。中古車プラザ / 買取専門 / 輸入車専門の 3 業態。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,店舗マスタ生成
PREFECTURES = [
    ("北海道", "札幌市", 43.06, 141.35),
    ("宮城県", "仙台市", 38.27, 140.87),
    ("東京都", "世田谷区", 35.65, 139.65),
    ("東京都", "練馬区", 35.74, 139.65),
    ("東京都", "足立区", 35.78, 139.81),
    ("神奈川県", "横浜市", 35.45, 139.64),
    ("神奈川県", "川崎市", 35.53, 139.71),
    ("千葉県", "千葉市", 35.61, 140.12),
    ("埼玉県", "さいたま市", 35.86, 139.65),
    ("茨城県", "つくば市", 36.08, 140.11),
    ("栃木県", "宇都宮市", 36.55, 139.88),
    ("群馬県", "前橋市", 36.39, 139.06),
    ("新潟県", "新潟市", 37.92, 139.04),
    ("長野県", "長野市", 36.65, 138.18),
    ("静岡県", "静岡市", 34.98, 138.38),
    ("愛知県", "名古屋市", 35.18, 136.91),
    ("愛知県", "岡崎市", 34.95, 137.17),
    ("岐阜県", "岐阜市", 35.42, 136.76),
    ("三重県", "四日市市", 34.97, 136.62),
    ("京都府", "京都市", 35.01, 135.76),
    ("大阪府", "大阪市", 34.69, 135.50),
    ("大阪府", "堺市", 34.57, 135.48),
    ("兵庫県", "神戸市", 34.69, 135.20),
    ("奈良県", "奈良市", 34.68, 135.83),
    ("広島県", "広島市", 34.39, 132.46),
    ("岡山県", "岡山市", 34.66, 133.93),
    ("福岡県", "福岡市", 33.59, 130.40),
    ("福岡県", "北九州市", 33.88, 130.88),
    ("熊本県", "熊本市", 32.79, 130.74),
    ("沖縄県", "那覇市", 26.21, 127.68),
]

FORMATS = ["中古車プラザ", "中古車プラザ", "中古車プラザ", "買取専門", "輸入車専門"]

stores = []
for i, (pref, city, lat, lon) in enumerate(PREFECTURES, start=1):
    fmt = random.choice(FORMATS)
    store_id = f"S{i:03d}"
    name = f"{city}{['本店','北','南','東','西','中央','駅前'][i % 7]}店"
    # 店舗ごとに緯度経度を少し散らす
    jitter_lat = lat + random.uniform(-0.05, 0.05)
    jitter_lon = lon + random.uniform(-0.05, 0.05)
    opened = dt.date(2010 + (i % 12), 1 + (i % 12), 1 + (i % 27))
    stores.append({
        "store_id": store_id,
        "store_name": name,
        "format": fmt,
        "prefecture": pref,
        "city": city,
        "latitude": round(jitter_lat, 5),
        "longitude": round(jitter_lon, 5),
        "opened_date": opened.isoformat(),
    })

stores_df = pd.DataFrame(stores)
out = f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/master/stores.csv"
stores_df.to_csv(out, index=False, encoding="utf-8")
print(f"✅ stores.csv: {len(stores_df)} 件 → {out}")
display(stores_df.head())

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 🚗 2. 車両在庫（vehicles）
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🚗 2. 車両在庫（vehicles）</strong><br>
# MAGIC 1,500 台。ボディタイプは <code>軽自動車 / コンパクト / セダン / SUV / ミニバン / 輸入車</code> の 6 区分（<code>market_index.車種カテゴリ</code> と同一語彙）。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,車両マスタ（メーカー × モデル × グレード）定義
# 国産・輸入それぞれの車種定義
VEHICLE_CATALOG = {
    "軽自動車": [
        ("ホンダ", "N-BOX", ["G・L", "カスタム L", "カスタム L ターボ"], (80, 200)),
        ("ダイハツ", "タント", ["L", "X", "カスタム RS"], (70, 180)),
        ("スズキ", "スペーシア", ["G", "HYBRID X", "カスタム HYBRID XS"], (75, 190)),
        ("ホンダ", "N-WGN", ["G", "L", "カスタム L"], (70, 170)),
        ("スズキ", "ワゴンR", ["FA", "HYBRID FX", "STINGRAY"], (65, 160)),
    ],
    "コンパクト": [
        ("トヨタ", "ヤリス", ["X", "G", "Z"], (130, 230)),
        ("日産", "ノート", ["X", "S", "AUTECH"], (140, 260)),
        ("ホンダ", "フィット", ["BASIC", "HOME", "CROSSTAR"], (130, 240)),
        ("トヨタ", "アクア", ["B", "X", "Z"], (160, 280)),
        ("マツダ", "MAZDA2", ["15S", "15S Touring", "XD"], (140, 260)),
    ],
    "セダン": [
        ("トヨタ", "カローラ", ["G-X", "S", "W×B"], (180, 320)),
        ("ホンダ", "シビック", ["EX", "LX"], (290, 420)),
        ("マツダ", "MAZDA3", ["15S", "20S", "X PROACTIVE"], (220, 380)),
        ("トヨタ", "プリウス", ["U", "G", "Z"], (250, 420)),
    ],
    "SUV": [
        ("トヨタ", "ハリアー", ["S", "G", "Z", "Z Leather Package"], (300, 600)),
        ("ホンダ", "ヴェゼル", ["G", "X", "Z", "e:HEV PLaY"], (240, 420)),
        ("トヨタ", "RAV4", ["X", "G", "Adventure"], (280, 480)),
        ("マツダ", "CX-5", ["20S", "25T", "XD L Package"], (270, 470)),
        ("日産", "エクストレイル", ["S", "X", "AUTECH e-4ORCE"], (320, 520)),
        ("スバル", "フォレスター", ["Touring", "X-BREAK", "Sport"], (290, 450)),
    ],
    "ミニバン": [
        ("トヨタ", "シエンタ", ["X", "G", "Z"], (200, 320)),
        ("ホンダ", "フリード", ["G", "CROSSTAR", "EX"], (220, 340)),
        ("トヨタ", "ノア", ["X", "G", "S-G"], (270, 400)),
        ("トヨタ", "ヴォクシー", ["S-G", "S-Z"], (310, 430)),
        ("ホンダ", "ステップワゴン", ["AIR", "SPADA", "e:HEV SPADA"], (300, 450)),
        ("トヨタ", "アルファード", ["X", "Z", "Executive Lounge"], (500, 900)),
    ],
    "輸入車": [
        ("BMW", "3シリーズ", ["320i", "330i M Sport", "M340i"], (350, 800)),
        ("メルセデス・ベンツ", "Cクラス", ["C200", "C220d AVANTGARDE", "C43 AMG"], (380, 850)),
        ("アウディ", "A4", ["35 TFSI", "40 TFSI advanced", "S4"], (340, 780)),
        ("フォルクスワーゲン", "ゴルフ", ["TSI Active", "TSI Style", "GTI"], (220, 480)),
        ("ボルボ", "XC60", ["B5 Momentum", "B5 Inscription", "T6 Recharge"], (450, 850)),
        ("プジョー", "308", ["Allure", "GT", "GT BlueHDi"], (260, 450)),
    ],
}

# COMMAND ----------

# DBTITLE 1,車両在庫データ生成
def gen_vin():
    """簡易 VIN（17 桁、I/O/Q を除く英数字）"""
    chars = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"
    return "".join(random.choices(chars, k=17))

def gen_plate():
    """簡易ナンバープレート"""
    region = random.choice(["品川", "練馬", "横浜", "札幌", "仙台", "名古屋", "大阪", "神戸", "福岡", "沖縄"])
    cls = random.choice(["300", "500", "330", "530"])
    hira = random.choice(["あ", "い", "う", "え", "か", "さ", "た", "な", "は", "ま"])
    num = f"{random.randint(1, 99):02d}-{random.randint(1, 99):02d}"
    return f"{region} {cls} {hira} {num}"

vehicles = []
TODAY = dt.date(2026, 5, 7)

for i in range(1, 1501):
    body_type = random.choice(list(VEHICLE_CATALOG.keys()))
    maker, model, grades, price_range = random.choice(VEHICLE_CATALOG[body_type])
    grade = random.choice(grades)

    # 年式：直近 7 年に偏らせる
    year = TODAY.year - int(np.random.choice(range(1, 8), p=[0.05, 0.15, 0.20, 0.20, 0.15, 0.15, 0.10]))
    age_years = TODAY.year - year
    # 走行距離：年式と相関（年 1 万 km 前後 + ばらつき）
    mileage = max(500, int(np.random.normal(age_years * 10000, 4000)))

    # 修復歴：あり 8%
    repair = "あり" if random.random() < 0.08 else "なし"
    # 認定中古車：あり 30%
    certified = bool(random.random() < 0.30)

    # 仕入価格：価格レンジ × 0.55〜0.75
    listing = int(random.uniform(*price_range)) * 10000
    purchase = int(listing * random.uniform(0.55, 0.75))

    # ステータス分布：在庫中 60% / 商談中 15% / 成約済 15% / 出庫済 10%
    status = random.choices(
        ["在庫中", "商談中", "成約済", "出庫済"],
        weights=[0.60, 0.15, 0.15, 0.10],
    )[0]

    # 入庫日：過去 1 年
    stock_in = TODAY - dt.timedelta(days=random.randint(1, 365))

    vehicles.append({
        "vehicle_id": f"V{i:05d}",
        "maker": maker,
        "model": model,
        "body_type": body_type,
        "model_year": year,
        "mileage_km": mileage,
        "repair_history": repair,
        "certified": certified,
        "vin": gen_vin(),
        "plate_number": gen_plate(),
        "grade": grade,
        "purchase_price": purchase,
        "listing_price": listing,
        "stock_in_date": stock_in.isoformat(),
        "status": status,
        "store_id": f"S{random.randint(1, 30):03d}",
    })

# --- Expectations デモ用：意図的な違反データを少量混入 ---
# SDP UI の Data quality タブで Failing records の件数を可視化するため
_bad_rng = random.Random(SEED + 999)
for j in range(5):
    # VIN 長さ違反（16 or 18 桁）→ vin_length_17 違反（DROP ROW）
    bad_len = _bad_rng.choice([16, 18])
    bad_vin = "".join(_bad_rng.choices("ABCDEFGHJKLMNPRSTUVWXYZ0123456789", k=bad_len))
    vehicles.append({
        "vehicle_id": f"V900{j+1:02d}", "maker": "トヨタ", "model": "ヤリス", "body_type": "コンパクト",
        "model_year": 2022, "mileage_km": 45000, "repair_history": "なし", "certified": False,
        "vin": bad_vin, "plate_number": gen_plate(), "grade": "G",
        "purchase_price": 1300000, "listing_price": 1800000,
        "stock_in_date": "2025-12-01", "status": "在庫中",
        "store_id": f"S{_bad_rng.randint(1, 30):03d}",
    })
for j in range(3):
    # 年式範囲外（1985 or 2030）→ model_year_in_range 違反（DROP ROW）
    bad_year = _bad_rng.choice([1985, 2030])
    vehicles.append({
        "vehicle_id": f"V910{j+1:02d}", "maker": "ホンダ", "model": "フィット", "body_type": "コンパクト",
        "model_year": bad_year, "mileage_km": 80000, "repair_history": "なし", "certified": False,
        "vin": gen_vin(), "plate_number": gen_plate(), "grade": "BASIC",
        "purchase_price": 800000, "listing_price": 1200000,
        "stock_in_date": "2024-06-01", "status": "在庫中",
        "store_id": f"S{_bad_rng.randint(1, 30):03d}",
    })
print(f"   ↳ Expectations デモ用の意図的違反データを 8 件混入（VIN 長さ違反 5 件 + 年式範囲外 3 件）")

vehicles_df = pd.DataFrame(vehicles)
out = f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/master/vehicles.csv"
vehicles_df.to_csv(out, index=False, encoding="utf-8")
print(f"✅ vehicles.csv: {len(vehicles_df)} 件 → {out}")
display(vehicles_df.head())

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 👤 3. 顧客マスタ（customers）
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>👤 3. 顧客マスタ（customers）</strong><br>
# MAGIC 2,500 名。氏名・電話番号は PII マスキング対象。<br>
# MAGIC 年収レンジ・家族構成・ライフステージ・希望ボディタイプ・予算上限の <b>一部は意図的に NULL</b> にしておき、後続 NB の <code>ai_query</code> で <code>inquiries.inquiry_memo</code> から抽出する例として使います。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,顧客マスタ生成
SURNAMES = ["佐藤", "鈴木", "高橋", "田中", "渡辺", "伊藤", "山本", "中村", "小林", "加藤",
            "吉田", "山田", "佐々木", "山口", "松本", "井上", "木村", "林", "斎藤", "清水"]
GIVEN_M = ["翔太", "健一", "大輔", "拓也", "直樹", "雅彦", "誠", "健太", "光", "昇"]
GIVEN_F = ["優子", "美咲", "里奈", "由美", "彩", "麻衣", "桃子", "愛", "綾子", "雅子"]

INCOME_RANGES = ["〜400万", "400-600万", "600-800万", "800-1000万", "1000万〜"]
FAMILY_STRUCT = ["独身", "夫婦のみ", "子育て世代", "子育て後", "シニア"]
LIFESTAGES = ["新社会人", "結婚適齢期", "子育て世代", "子育て後", "シニア"]
BODY_TYPES = list(VEHICLE_CATALOG.keys())

customers = []
for i in range(1, 2501):
    gender = random.choice(["M", "F"])
    given = random.choice(GIVEN_M if gender == "M" else GIVEN_F)
    name = f"{random.choice(SURNAMES)} {given}"
    age = int(np.clip(np.random.normal(45, 15), 20, 80))

    phone = f"0{random.choice([3, 6, 11, 22, 45, 52, 75, 78, 92])}-{random.randint(1000,9999)}-{random.randint(1000,9999)}"

    pref, city, _, _ = random.choice(PREFECTURES)
    area = f"{pref}{city}"

    first_contact = TODAY - dt.timedelta(days=random.randint(30, 1095))

    # 一部の顧客は属性 NULL にして AI Functions enrich の余地を残す
    has_profile = random.random() < 0.55  # 55% は属性入り、45% は NULL（後で ai_query 補完）

    customers.append({
        "customer_id": f"C{i:05d}",
        "name": name,
        "age": age,
        "gender": gender,
        "phone": phone,
        "area": area,
        "registered_store_id": f"S{random.randint(1, 30):03d}",
        "first_contact_date": first_contact.isoformat(),
        "annual_income_range": random.choice(INCOME_RANGES) if has_profile else None,
        "family_structure": random.choice(FAMILY_STRUCT) if has_profile else None,
        "lifestage": random.choice(LIFESTAGES) if has_profile else None,
        "preferred_body_type": random.choice(BODY_TYPES) if has_profile else None,
        "budget_max": random.choice([150, 200, 250, 300, 400, 500, 600, 800, 1000, 1500]) * 10000 if has_profile else None,
    })

# --- Expectations デモ用：意図的な違反データを少量混入 ---
# SDP UI の Data quality タブで Failing records の件数を可視化するため
_bad_rng = random.Random(SEED + 998)
for j in range(5):
    # 年齢範囲外（15 or 105）→ age_in_range 違反（DROP ROW）
    bad_age = _bad_rng.choice([15, 105])
    customers.append({
        "customer_id": f"C9{j+1:04d}",
        "name": "テスト 太郎",
        "age": bad_age,
        "gender": "M",
        "phone": "0000-0000-0000",
        "area": "東京都世田谷区",
        "registered_store_id": f"S{_bad_rng.randint(1, 30):03d}",
        "first_contact_date": "2024-01-01",
        "annual_income_range": None,
        "family_structure": None,
        "lifestage": None,
        "preferred_body_type": None,
        "budget_max": None,
    })
for j in range(5):
    # gender='X' → valid_gender 警告のみ（DROP ROW なし）
    customers.append({
        "customer_id": f"C8{j+1:04d}",
        "name": "テスト 花子",
        "age": 30,
        "gender": "X",
        "phone": "0000-0000-0000",
        "area": "東京都世田谷区",
        "registered_store_id": f"S{_bad_rng.randint(1, 30):03d}",
        "first_contact_date": "2024-01-01",
        "annual_income_range": None,
        "family_structure": None,
        "lifestage": None,
        "preferred_body_type": None,
        "budget_max": None,
    })
print(f"   ↳ Expectations デモ用の意図的違反データを 10 件混入（年齢範囲外 5 件 + gender='X' 5 件）")

customers_df = pd.DataFrame(customers)
out = f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/master/customers.csv"
customers_df.to_csv(out, index=False, encoding="utf-8")
print(f"✅ customers.csv: {len(customers_df)} 件 (うち属性 NULL: {customers_df['annual_income_range'].isna().sum()}) → {out}")
display(customers_df.head())

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 📈 4. 中古車相場指数（market_index）
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📈 4. 中古車相場指数（market_index）</strong><br>
# MAGIC 過去 2 年分（730 日）。車種カテゴリ別の相場指数（基準=1.0）、レギュラー価格、為替 USD/JPY。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,相場指数データ生成
start_date = TODAY - dt.timedelta(days=730)
market = []
# カテゴリごとに緩やかなトレンド + 日次ノイズ
for cat in BODY_TYPES:
    base = random.uniform(0.95, 1.05)
    trend = random.uniform(-0.0003, 0.0005)  # 日次トレンド
    for d in range(730):
        date = start_date + dt.timedelta(days=d)
        idx = base + trend * d + np.random.normal(0, 0.015)
        market.append({
            "date": date.isoformat(),
            "vehicle_category": cat,
            "market_index": round(idx, 4),
            "regular_gas_price": round(160 + np.random.normal(0, 8) + 0.01 * d, 1),
            "usd_jpy_rate": round(140 + np.random.normal(0, 5) + 0.005 * d, 2),
        })

market_df = pd.DataFrame(market)
out = f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/master/market_index.csv"
market_df.to_csv(out, index=False, encoding="utf-8")
print(f"✅ market_index.csv: {len(market_df)} 件 → {out}")
display(market_df.head())

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 🗣 5. 商談履歴（inquiries）
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🗣 5. 商談履歴（inquiries）+ 商談メモ</strong><br>
# MAGIC 12,500 件。<code>inquiry_memo</code> はフリーテキストで、後続 NB の <code>ai_query</code> による顧客プロファイル抽出の入力になります。<br>
# MAGIC <b>履歴バルク 1 ファイル + 直近 5 日分の日次 CSV 5 ファイル</b> に分割。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,商談メモ生成テンプレート
MEMO_TEMPLATES = [
    "{family}で乗れる{body}を探しています。予算は{budget}万円程度で、{concern}を重視したいです。{timing}までに納車できると助かります。",
    "現在乗っている{old_car}から乗り換え検討中です。{reason}のため{body}が気になっています。{lifestage_hint}",
    "週末に来店しました。{body}を中心に見ています。{family_hint}{budget_hint}",
    "{income_hint}{body}希望でローン審査を相談されました。{family_hint}走行距離より年式重視。",
    "下取りありで査定済み。{old_car}を{trade_in}万円で。買い替え先は{body}を希望。{concern}が条件。",
    "{lifestage_hint}{body}の{grade_hint}を比較中。納期と総支払額の見積もりを依頼。",
    "オンラインで問い合わせ後に来店。{body}の{spec}について質問あり。{family_hint}",
    "{family}向けに{body}を提案。{budget_hint}{concern}の優先度が高い。",
    "認定中古車の説明を希望。{body}でできるだけ走行距離が少ないものを希望。{income_hint}",
    "{lifestage_hint}維持費の相談あり。燃費の良い{body}を提示。{budget_hint}",
]

FAMILY_PHRASES = ["家族 4 人", "家族 5 人", "夫婦 2 人", "夫婦と子供 1 人", "1 人"]
BODY_PHRASES = ["コンパクト", "SUV", "ミニバン", "セダン", "軽自動車", "輸入車"]
CONCERN_PHRASES = ["燃費", "安全装備", "後部座席の広さ", "荷室容量", "走行性能", "見た目"]
TIMING_PHRASES = ["来月", "ボーナス時期", "年度内", "今月末", "夏休み前"]
OLD_CAR_PHRASES = ["10 年落ちのセダン", "軽自動車", "ミニバン", "コンパクトカー", "SUV"]
REASON_PHRASES = ["子供が生まれる予定", "通勤が始まる", "退職して時間ができた", "家族が増えた", "車検切れ間近"]
LIFESTAGE_HINTS = [
    "結婚を機に車を検討中。", "子供の送り迎え用に必要。", "新社会人で初めての車購入。",
    "退職金で念願の車を購入予定。", "子供が独立してセカンドカー検討。", "新生活に向けて車が必要。",
    "",
]
INCOME_HINTS = ["年収 500 万前後とのこと。", "世帯年収 800 万との情報。", "年収 1000 万超え。", "ローン枠 300 万まで。", ""]
FAMILY_HINTS = ["お子様 2 人。", "ご夫婦のみ。", "ペットありで荷室を重視。", "三世代同居。", ""]
BUDGET_HINTS = ["予算上限 300 万。", "総額 500 万以内。", "月々 5 万までで検討。", "頭金 100 万あり。", ""]
GRADE_HINTS = ["上位グレード", "標準グレード", "ハイブリッドモデル", "ターボモデル", "エントリーグレード"]
SPEC_PHRASES = ["WLTC 燃費", "安全装備の有無", "車検残期間", "純正ナビ", "禁煙車かどうか"]

def gen_memo():
    tpl = random.choice(MEMO_TEMPLATES)
    return tpl.format(
        family=random.choice(FAMILY_PHRASES),
        body=random.choice(BODY_PHRASES),
        budget=random.choice([200, 300, 400, 500, 600, 800]),
        concern=random.choice(CONCERN_PHRASES),
        timing=random.choice(TIMING_PHRASES),
        old_car=random.choice(OLD_CAR_PHRASES),
        reason=random.choice(REASON_PHRASES),
        lifestage_hint=random.choice(LIFESTAGE_HINTS),
        income_hint=random.choice(INCOME_HINTS),
        family_hint=random.choice(FAMILY_HINTS),
        budget_hint=random.choice(BUDGET_HINTS),
        grade_hint=random.choice(GRADE_HINTS),
        spec=random.choice(SPEC_PHRASES),
        trade_in=random.choice([30, 50, 80, 120, 200]),
    )

print("商談メモ生成例:")
for _ in range(3):
    print(" -", gen_memo())

# COMMAND ----------

# DBTITLE 1,商談履歴データ生成
CHANNELS = ["来店", "オンライン", "買取査定", "電話"]
INQ_STATUS = ["商談中", "成約", "失注"]

inquiries = []
for i in range(1, 12501):
    inq_date = TODAY - dt.timedelta(days=random.randint(0, 365))
    channel = random.choices(CHANNELS, weights=[0.55, 0.20, 0.15, 0.10])[0]
    status = random.choices(INQ_STATUS, weights=[0.30, 0.40, 0.30])[0]
    inquiries.append({
        "inquiry_id": f"Q{i:06d}",
        "customer_id": f"C{random.randint(1, 2500):05d}",
        "store_id": f"S{random.randint(1, 30):03d}",
        "vehicle_id": f"V{random.randint(1, 1500):05d}",
        "inquiry_date": inq_date.isoformat(),
        "channel": channel,
        "status": status,
        "inquiry_memo": gen_memo(),
    })

inq_df = pd.DataFrame(inquiries)
inq_df["inquiry_date_dt"] = pd.to_datetime(inq_df["inquiry_date"])

# 直近 5 日分（2026-05-02〜2026-05-06）を日次ファイルに、残りを履歴バルクに
recent_5 = [TODAY - dt.timedelta(days=d) for d in range(5, 0, -1)]
recent_set = set(d.isoformat() for d in recent_5)

history_df = inq_df[~inq_df["inquiry_date"].isin(recent_set)].drop(columns=["inquiry_date_dt"])
history_path = f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/transactions/inquiries/inquiries_history.csv"
history_df.to_csv(history_path, index=False, encoding="utf-8")
print(f"✅ inquiries_history.csv: {len(history_df)} 件 → {history_path}")

for d in recent_5:
    daily = inq_df[inq_df["inquiry_date"] == d.isoformat()].drop(columns=["inquiry_date_dt"])
    daily_path = f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/transactions/inquiries/inquiries_{d.isoformat()}.csv"
    daily.to_csv(daily_path, index=False, encoding="utf-8")
    print(f"   ↳ inquiries_{d.isoformat()}.csv: {len(daily)} 件")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 📝 6. 成約・粗利（contracts）
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📝 6. 成約・粗利（contracts）</strong><br>
# MAGIC 5,000 件。inquiries の <code>成約</code> ステータスから一部を契約化。<br>
# MAGIC こちらも <b>履歴バルク + 直近 5 日分の日次 CSV</b> に分割。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,成約データ生成
# 成約済みの inquiry をベースに contract を作る
won_inq = inq_df[inq_df["status"] == "成約"].sample(n=5000, random_state=SEED, replace=False)
PAYMENT = ["現金", "ローン", "リース"]

contracts = []
for i, row in enumerate(won_inq.itertuples(), start=1):
    veh = vehicles_df.loc[vehicles_df["vehicle_id"] == row.vehicle_id].iloc[0]
    list_p = veh["listing_price"]
    purchase_p = veh["purchase_price"]
    # 値引き 0〜5%
    actual_price = int(list_p * random.uniform(0.95, 1.0))
    gross = actual_price - purchase_p
    margin = round(gross / actual_price, 4) if actual_price > 0 else 0.0

    # 契約日 = 商談日 + 0〜10 日
    inq_dt = dt.date.fromisoformat(row.inquiry_date)
    contract_dt = inq_dt + dt.timedelta(days=random.randint(0, 10))
    if contract_dt > TODAY:
        contract_dt = TODAY

    contracts.append({
        "contract_id": f"K{i:06d}",
        "inquiry_id": row.inquiry_id,
        "customer_id": row.customer_id,
        "store_id": row.store_id,
        "vehicle_id": row.vehicle_id,
        "contract_date": contract_dt.isoformat(),
        "listing_price": list_p,
        "actual_price": actual_price,
        "purchase_price": purchase_p,
        "gross_profit": gross,
        "gross_margin": margin,
        "payment_method": random.choices(PAYMENT, weights=[0.20, 0.65, 0.15])[0],
    })

# --- Expectations デモ用：意図的な違反データを少量混入 ---
# margin_consistency: gross_profit を意図的にズラして「販売価格 - 仕入価格」と一致させない
_bad_rng = random.Random(SEED + 997)
for j in range(10):
    base = contracts[_bad_rng.randint(0, len(contracts) - 1)].copy()
    base["contract_id"] = f"K9{j+1:05d}"
    base["gross_profit"] = (
        base["actual_price"] - base["purchase_price"]
        + _bad_rng.choice([10000, -10000, 5000, -5000])
    )
    contracts.append(base)
print(f"   ↳ Expectations デモ用の意図的違反データを 10 件混入（粗利整合性違反）")

contracts_df = pd.DataFrame(contracts)

history_df = contracts_df[~contracts_df["contract_date"].isin(recent_set)]
history_path = f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/transactions/contracts/contracts_history.csv"
history_df.to_csv(history_path, index=False, encoding="utf-8")
print(f"✅ contracts_history.csv: {len(history_df)} 件 → {history_path}")

for d in recent_5:
    daily = contracts_df[contracts_df["contract_date"] == d.isoformat()]
    daily_path = f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/transactions/contracts/contracts_{d.isoformat()}.csv"
    daily.to_csv(daily_path, index=False, encoding="utf-8")
    print(f"   ↳ contracts_{d.isoformat()}.csv: {len(daily)} 件")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 📄 7. 車両カタログ PDF
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📄 7. 車両カタログ PDF（5 車種）</strong><br>
# MAGIC ハリアー / ヴェゼル / シエンタ / ノート / N-BOX の 4 グレード相当。<br>
# MAGIC 諸元・WLTC 燃費・グレード別装備・価格帯（架空）を記載。後続 NB で <code>ai_parse_document</code> → <code>ai_prep_search</code> → Vector Search Index に流す入力です。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,PDF カタログ定義（架空データ）
CATALOGS = {
    "harrier": {
        "title": "ハリアー カタログ（2026 モデル）",
        "maker": "トヨタ", "model": "ハリアー", "body_type": "SUV",
        "overview": "都会的なエクステリアと上質なインテリアを両立したプレミアム SUV。ハイブリッドモデルでは WLTC モード 22.3km/L の高い燃費性能を実現。",
        "dimensions": "全長 4,740mm × 全幅 1,855mm × 全高 1,660mm、ホイールベース 2,690mm",
        "engine": "2.0L ガソリン（171ps）/ 2.5L ハイブリッド（システム最高出力 218ps）",
        "wltc": {"S": "15.4 km/L", "G": "15.4 km/L", "Z": "22.3 km/L (HEV)", "Z Leather Package": "22.3 km/L (HEV)"},
        "price": {"S": "312 万円〜", "G": "362 万円〜", "Z": "458 万円〜", "Z Leather Package": "504 万円〜"},
        "equipment": {
            "S": "LED ヘッドライト、17 インチアルミ、Toyota Safety Sense",
            "G": "S 装備 + 18 インチアルミ、合成皮革シート、シートヒーター",
            "Z": "G 装備 + 19 インチアルミ、本革ステアリング、デジタルインナーミラー、調光パノラマルーフ",
            "Z Leather Package": "Z 装備 + 本革シート、運転席パワーシート、ベンチレーション機能",
        },
    },
    "vezel": {
        "title": "ヴェゼル カタログ（2026 モデル）",
        "maker": "ホンダ", "model": "ヴェゼル", "body_type": "SUV",
        "overview": "Honda のコンパクト SUV。e:HEV はモーター主体の走りで WLTC モード 25.0km/L を達成。",
        "dimensions": "全長 4,330mm × 全幅 1,790mm × 全高 1,580mm、ホイールベース 2,610mm",
        "engine": "1.5L ガソリン（118ps）/ 1.5L e:HEV（システム最高出力 131ps）",
        "wltc": {"G": "17.0 km/L", "X": "17.0 km/L", "Z": "25.0 km/L (e:HEV)", "e:HEV PLaY": "22.0 km/L"},
        "price": {"G": "239.9 万円〜", "X": "265.7 万円〜", "Z": "318.9 万円〜", "e:HEV PLaY": "359.9 万円〜"},
        "equipment": {
            "G": "Honda SENSING、LED ヘッドライト、16 インチアルミ",
            "X": "G 装備 + コネクト 8 インチナビ、シートヒーター（運転席・助手席）",
            "Z": "X 装備 + 18 インチアルミ、本革巻きステアリング、Honda CONNECT for Gathers",
            "e:HEV PLaY": "Z 装備 + パノラマルーフ、専用インテリア、ボディーカラー専用設定",
        },
    },
    "sienta": {
        "title": "シエンタ カタログ（2026 モデル）",
        "maker": "トヨタ", "model": "シエンタ", "body_type": "ミニバン",
        "overview": "5 人乗り / 7 人乗り 2 タイプを設定するコンパクトミニバン。ハイブリッドの WLTC モード 28.4km/L はクラストップレベル。",
        "dimensions": "全長 4,260mm × 全幅 1,695mm × 全高 1,695mm、ホイールベース 2,750mm",
        "engine": "1.5L ガソリン（120ps）/ 1.5L ハイブリッド（システム最高出力 116ps）",
        "wltc": {"X": "18.3 km/L", "G": "28.4 km/L (HEV)", "Z": "28.4 km/L (HEV)"},
        "price": {"X": "199.5 万円〜", "G": "245.0 万円〜", "Z": "278.0 万円〜"},
        "equipment": {
            "X": "Toyota Safety Sense、LED ヘッドライト、5 人乗り",
            "G": "X 装備 + 7 人乗り選択可、両側電動スライドドア、ディスプレイオーディオ",
            "Z": "G 装備 + 16 インチアルミ、合成皮革シート、シートヒーター、自動駐車支援",
        },
    },
    "note": {
        "title": "ノート カタログ（2026 モデル）",
        "maker": "日産", "model": "ノート", "body_type": "コンパクト",
        "overview": "全車 e-POWER 専用。モーター駆動による静粛性と滑らかな加速、WLTC モード 28.4km/L の燃費性能。",
        "dimensions": "全長 4,045mm × 全幅 1,695mm × 全高 1,520mm、ホイールベース 2,580mm",
        "engine": "1.2L e-POWER（システム最高出力 116ps）",
        "wltc": {"X": "28.4 km/L", "S": "28.4 km/L", "AUTECH": "28.4 km/L"},
        "price": {"X": "229.9 万円〜", "S": "204.9 万円〜", "AUTECH": "265.0 万円〜"},
        "equipment": {
            "X": "プロパイロット、LED ヘッドライト、9 インチナビ",
            "S": "ベーシック装備、ハロゲンヘッドライト、6 スピーカー",
            "AUTECH": "X 装備 + 専用インテリア、16 インチアルミ、専用エンブレム",
        },
    },
    "n_box": {
        "title": "N-BOX カタログ（2026 モデル）",
        "maker": "ホンダ", "model": "N-BOX", "body_type": "軽自動車",
        "overview": "軽自動車販売台数 No.1 のスーパーハイトワゴン。広々とした室内空間と充実の安全装備。",
        "dimensions": "全長 3,395mm × 全幅 1,475mm × 全高 1,790mm、ホイールベース 2,520mm",
        "engine": "660cc NA（58ps）/ 660cc ターボ（64ps）",
        "wltc": {"G・L": "21.6 km/L", "カスタム L": "21.5 km/L", "カスタム L ターボ": "20.3 km/L"},
        "price": {"G・L": "164.8 万円〜", "カスタム L": "184.8 万円〜", "カスタム L ターボ": "199.8 万円〜"},
        "equipment": {
            "G・L": "Honda SENSING、LED ヘッドライト、両側スライドドア（左電動）",
            "カスタム L": "G・L 装備 + 14 インチアルミ、専用フロントグリル、両側電動スライドドア",
            "カスタム L ターボ": "カスタム L 装備 + ターボエンジン、本革巻きステアリング、パドルシフト",
        },
    },
}

# COMMAND ----------

# DBTITLE 1,PDF カタログ生成
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib import colors

# 日本語 TTF フォント（IPAex Gothic を PDF に埋め込み）
import japanize_matplotlib
import os
JP_FONT_PATH = os.path.join(os.path.dirname(japanize_matplotlib.__file__), "fonts", "ipaexg.ttf")
pdfmetrics.registerFont(TTFont("IPAexGothic", JP_FONT_PATH))
JP_FONT = "IPAexGothic"

styles = getSampleStyleSheet()
title_style = ParagraphStyle("title_jp", parent=styles["Heading1"], fontName=JP_FONT, fontSize=20, leading=26)
h2_style = ParagraphStyle("h2_jp", parent=styles["Heading2"], fontName=JP_FONT, fontSize=14, leading=20, textColor=colors.HexColor("#1B3139"))
body_style = ParagraphStyle("body_jp", parent=styles["BodyText"], fontName=JP_FONT, fontSize=10.5, leading=16)

def build_pdf(key, spec, out_path):
    doc = SimpleDocTemplate(out_path, pagesize=A4, leftMargin=20*mm, rightMargin=20*mm, topMargin=20*mm, bottomMargin=20*mm)
    flow = []
    flow.append(Paragraph(spec["title"], title_style))
    flow.append(Spacer(1, 6*mm))

    flow.append(Paragraph(f"メーカー：{spec['maker']}　／　モデル：{spec['model']}　／　ボディタイプ：{spec['body_type']}", body_style))
    flow.append(Spacer(1, 4*mm))

    flow.append(Paragraph("■ 車両概要", h2_style))
    flow.append(Paragraph(spec["overview"], body_style))
    flow.append(Spacer(1, 4*mm))

    flow.append(Paragraph("■ 主要諸元", h2_style))
    flow.append(Paragraph(f"寸法：{spec['dimensions']}", body_style))
    flow.append(Paragraph(f"エンジン：{spec['engine']}", body_style))
    flow.append(Spacer(1, 4*mm))

    flow.append(Paragraph("■ グレード別 WLTC 燃費", h2_style))
    table_data = [["グレード", "WLTC 燃費"]]
    for g, v in spec["wltc"].items():
        table_data.append([g, v])
    t = Table(table_data, colWidths=[60*mm, 60*mm])
    t.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), JP_FONT),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E3F2FD")),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("PADDING", (0, 0), (-1, -1), 6),
    ]))
    flow.append(t)
    flow.append(Spacer(1, 4*mm))

    flow.append(Paragraph("■ グレード別 価格", h2_style))
    table_data = [["グレード", "メーカー希望小売価格"]]
    for g, v in spec["price"].items():
        table_data.append([g, v])
    t = Table(table_data, colWidths=[60*mm, 60*mm])
    t.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), JP_FONT),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E8F5E9")),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("PADDING", (0, 0), (-1, -1), 6),
    ]))
    flow.append(t)
    flow.append(Spacer(1, 4*mm))

    flow.append(Paragraph("■ グレード別 主要装備", h2_style))
    for g, v in spec["equipment"].items():
        flow.append(Paragraph(f"<b>{g}</b>：{v}", body_style))
        flow.append(Spacer(1, 2*mm))

    doc.build(flow)

for key, spec in CATALOGS.items():
    out_path = f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}/pdf_catalogs/{key}.pdf"
    build_pdf(key, spec, out_path)
    print(f"✅ {key}.pdf → {out_path}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 音声ファイル(MP3)を Volume に配置
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">音声ファイル(MP3)を Volume に配置</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC ノートブックと同じ階層の <code>_audio/</code> フォルダに置いた商談録音(MP3)を、<br>
# MAGIC Volume の <code>audio/</code> 配下にコピーします。<br>
# MAGIC 04 の Whisper 文字起こしの入力になります。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,_audio/ フォルダの MP3 を Volume にコピー
import os, shutil

# このノートブックの親ディレクトリ(workspace 上)を取得
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
ws_root = "/Workspace" + os.path.dirname(nb_path)
audio_src = f"{ws_root}/_audio"
audio_dst = f"{VOLUME_PATH}/audio"

os.makedirs(audio_dst, exist_ok=True)

if not os.path.isdir(audio_src):
    print(f"⚠️  音声ソースフォルダが見つかりません: {audio_src}")
    print(f"   ノートブックと同じ階層に '_audio' フォルダを作成し、MP3 を配置してください。")
else:
    copied = 0
    for f in sorted(os.listdir(audio_src)):
        if f.lower().endswith(".mp3"):
            src = os.path.join(audio_src, f)
            dst = os.path.join(audio_dst, f)
            shutil.copy2(src, dst)
            size_kb = os.path.getsize(dst) / 1024
            print(f"✅ {f}  ({size_kb:,.1f} KB) → {audio_dst}")
            copied += 1
    print(f"\n📦 合計 {copied} 件の MP3 を Volume にコピーしました")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ Volume 配置の確認</strong>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Volume 配下のファイル一覧
for sub in SUBDIRS:
    target = f"{VOLUME_PATH}/{sub}"
    print(f"\n📁 {target}")
    for f in dbutils.fs.ls(target):
        size_mb = f.size / (1024 * 1024)
        print(f"   {f.name:<45} {size_mb:>8.2f} MB")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🎉 データ準備完了</strong><br>
# MAGIC 次は <code>02_SDPパイプライン定義.py</code> + <code>03_SDPパイプライン設定手順.py</code> で SDP + Autoloader + Expectations を構築します。
# MAGIC </div>
