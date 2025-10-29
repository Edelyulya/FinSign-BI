import json
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

# ---------- Config ----------
ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config.json"

if not CONFIG_PATH.exists():
    raise FileNotFoundError("Файл config.json не найден в корне проекта.")

with CONFIG_PATH.open("r", encoding="utf-8") as f:
    cfg = json.load(f)

db = cfg["db"]
ozon = cfg["ozon_api"]

# ---------- API settings ----------
URL = "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"
HEADERS = {
    "Client-Id": ozon["client_id"],
    "Api-Key": ozon["api_key"],
}
TIMEOUT = 30  # сек

def run_etl():
    print("🚀 Запуск ETL из Ozon API...")

    try:
        resp = requests.post(URL, json={}, headers=HEADERS, timeout=TIMEOUT)
    except requests.RequestException as e:
        print(f"❌ Сетевой сбой при обращении к Ozon API: {e}")
        return

    if resp.status_code != 200:
        print(f"❌ Ошибка API: {resp.status_code}\n{resp.text}")
        return

    try:
        data = resp.json()
    except ValueError as e:
        print(f"❌ Невалидный JSON от API: {e}")
        return

    df = pd.json_normalize(data.get("result", []))
    if df.empty:
        print("⚠️ Ozon API вернул пустой результат.")
        return

    # ---------- DB engine ----------
    conn_str = (
        f"postgresql+psycopg://{db['user']}:{db['password']}"
        f"@{db['host']}:{db['port']}/{db['database']}"
    )
    try:
        engine = create_engine(conn_str, pool_pre_ping=True)
        df.to_sql("ozon_stock", engine, schema="raw", if_exists="replace", index=False)
    except OperationalError as e:
        print(f"❌ Не удалось подключиться к PostgreSQL: {e}")
        return
    except Exception as e:
        print(f"❌ Ошибка записи в БД: {e}")
        return

    print(f"✅ Загружено {len(df)} строк в таблицу raw.ozon_stock")

if __name__ == "__main__":
    run_etl()
