from sqlalchemy import text

def bootstrap_raw(engine):
    ddl = """
    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE TABLE IF NOT EXISTS raw.etl_log (
      id bigserial PRIMARY KEY,
      source text NOT NULL,
      endpoint text NOT NULL,
      started_at timestamptz NOT NULL DEFAULT now(),
      finished_at timestamptz,
      status text NOT NULL DEFAULT 'running',
      rows_loaded integer DEFAULT 0,
      message text
    );

    CREATE TABLE IF NOT EXISTS raw.ozon_stock_raw (
      id bigserial PRIMARY KEY,
      loaded_at timestamptz NOT NULL DEFAULT now(),
      payload jsonb NOT NULL
    );

    CREATE TABLE IF NOT EXISTS raw.ozon_stock (
      loaded_at timestamptz NOT NULL DEFAULT now(),
      date date,
      warehouse_name text,
      region text,
      product_id text,
      sku text,
      item_name text,
      quantity numeric,
      reserved numeric,
      price numeric
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

import argparse
import json
import time
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

import pandas as pd
import requests
from sqlalchemy import create_engine, text


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config.json"

# ---------- helpers ----------
def load_config() -> Dict[str, Any]:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)

def make_engine(cfg: Dict[str, Any]):
    db = cfg["db"]
    conn_str = (
        f"postgresql+psycopg://{db['user']}:{db['password']}"
        f"@{db['host']}:{db['port']}/{db['database']}"
    )
    return create_engine(conn_str, pool_pre_ping=True)

def backoff(retry: int) -> None:
    delay = min(2 ** retry, 30)  # 1,2,4,8,16,30...
    time.sleep(delay)

# ---------- Ozon client ----------
OZON_URL_STOCK = "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"

def ozon_headers(cfg: Dict[str, Any]) -> Dict[str, str]:
    oz = cfg["ozon_api"]
    return {"Client-Id": oz["client_id"], "Api-Key": oz["api_key"]}

def post_with_retries(url: str, headers: Dict[str, str], payload: Dict[str, Any]) -> Dict[str, Any]:
    max_retries = 5
    for attempt in range(max_retries):
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=60)
            if resp.status_code == 200:
                return resp.json()
            else:
                msg = f"Ozon API HTTP {resp.status_code}: {resp.text[:300]}"
                # 5xx — пробуем ретраи, 4xx — сразу падаем (обычно неверный запрос/ключи)
                if 500 <= resp.status_code < 600:
                    backoff(attempt)
                    continue
                raise RuntimeError(msg)
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                raise
            backoff(attempt)
    # сюда не дойдём
    return {}

def extract_items(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Ответы Ozon бывают как массив, так и объект c полями.
       Пробуем вытащить массив максимально аккуратно."""
    if not isinstance(data, dict):
        return []
    res = data.get("result", data)
    if isinstance(res, list):
        return res
    if isinstance(res, dict):
        # самые частые варианты
        for key in ("items", "stocks", "data", "rows"):
            if isinstance(res.get(key), list):
                return res[key]
    return []

def fetch_all_ozon_stock(cfg: Dict[str, Any], page_limit: int = 1000) -> List[Dict[str, Any]]:
    """Пагинация по limit/offset до вычерпывания данных."""
    headers = ozon_headers(cfg)
    all_items: List[Dict[str, Any]] = []
    offset = 0

    while True:
        payload = {
            "limit": page_limit,   # важно: 1..1000
            "offset": offset
            # сюда можно добавить фильтры, если они поддерживаются
        }
        data = post_with_retries(OZON_URL_STOCK, headers, payload)
        batch = extract_items(data)
        batch_count = len(batch)
        print(f"• batch @ offset={offset}: {batch_count}")
        if batch_count == 0:
            break
        all_items.extend(batch)
        if batch_count < page_limit:
            break
        offset += page_limit

    return all_items

# ---------- normalization ----------
def normalize_stock(items: List[Dict[str, Any]]) -> pd.DataFrame:
    if not items:
        return pd.DataFrame()

    df = pd.json_normalize(items)

    # гарантируем наличие колонок
    for col in [
        "date", "updated_at", "warehouse_name", "region",
        "product_id", "sku", "item_name", "quantity", "reserved", "price"
    ]:
        if col not in df.columns:
            df[col] = None

    # дата
    if "date" in df:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    if df["date"].isna().all():
        df["date"] = pd.to_datetime(df.get("updated_at"), errors="coerce").dt.date
    if df["date"].isna().all():
        df["date"] = date.today()

    # числовые
    for num_col in ["quantity", "reserved", "price"]:
        df[num_col] = pd.to_numeric(df[num_col], errors="coerce").fillna(0)

    out = df[
        ["date", "warehouse_name", "region", "product_id", "sku", "item_name", "quantity", "reserved", "price"]
    ].copy()

    # унификация типов
    out["warehouse_name"] = out["warehouse_name"].astype("string")
    out["region"] = out["region"].astype("string")
    out["product_id"] = out["product_id"].astype("string")
    out["sku"] = out["sku"].astype("string")
    out["item_name"] = out["item_name"].astype("string")

    return out

# ---------- persistence ----------
def start_log(engine, source: str, endpoint: str) -> int:
    with engine.begin() as conn:
        res = conn.execute(
            text("INSERT INTO raw.etl_log (source, endpoint) VALUES (:s,:e) RETURNING id"),
            {"s": source, "e": endpoint},
        )
        return res.scalar_one()

def finish_log(engine, log_id: int, status: str, rows: int, message: str = None):
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE raw.etl_log
                   SET finished_at = now(),
                       status = :st,
                       rows_loaded = :rows,
                       message = :msg
                 WHERE id = :id
            """),
            {"st": status, "rows": rows, "msg": message, "id": log_id},
        )

def persist_raw(engine, payload_list: List[Dict[str, Any]]):
    if not payload_list:
        return 0
    df_raw = pd.DataFrame({"payload": payload_list})
    df_raw.to_sql("ozon_stock_raw", engine, schema="raw", if_exists="append", index=False)
    return len(df_raw)

def persist_normalized(engine, df_norm: pd.DataFrame):
    if df_norm.empty:
        return 0
    df_norm.to_sql("ozon_stock", engine, schema="raw", if_exists="append", index=False)
    return len(df_norm)

from sqlalchemy import text

def bootstrap_mart(engine):
    """Создаёт схему mart и таблицу fact_sales (если отсутствуют) + индексы и вью."""
    ddl = """
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.fact_sales (
    date        date            NOT NULL,
    marketplace text            NOT NULL DEFAULT 'ozon',
    sku         text            NOT NULL,
    region      text            NOT NULL DEFAULT '',
    revenue     numeric(18,2)   NOT NULL DEFAULT 0,
    cost        numeric(18,2)   NOT NULL DEFAULT 0,
    profit      numeric(18,2)   GENERATED ALWAYS AS (revenue - cost) STORED,
    PRIMARY KEY (date, marketplace, sku, region)
);

CREATE INDEX IF NOT EXISTS ix_fact_sales_date        ON mart.fact_sales(date);
CREATE INDEX IF NOT EXISTS ix_fact_sales_marketplace ON mart.fact_sales(marketplace);
CREATE INDEX IF NOT EXISTS ix_fact_sales_sku         ON mart.fact_sales(sku);
CREATE INDEX IF NOT EXISTS ix_fact_sales_region      ON mart.fact_sales(region);

CREATE OR REPLACE VIEW mart.vw_kpi AS
SELECT
    date,
    marketplace,
    SUM(revenue) AS revenue,
    SUM(cost)    AS cost,
    SUM(profit)  AS profit
FROM mart.fact_sales
GROUP BY 1,2;
"""

    with engine.begin() as conn:
        conn.execute(text(ddl))

def rebuild_mart_from_raw(engine):
    """Полная пересборка mart.fact_sales напрямую из raw.ozon_stock (без TEMP-таблиц)."""
    sql = """
    TRUNCATE TABLE mart.fact_sales;

    INSERT INTO mart.fact_sales (date, marketplace, sku, region, revenue, cost)
    SELECT
        COALESCE(date, CURRENT_DATE)::date                AS date,
        'ozon'::text                                      AS marketplace,
        COALESCE(sku, 'UNKNOWN')::text                    AS sku,
        COALESCE(NULLIF(region, ''), '')::text            AS region,
        (COALESCE(quantity, 0)::numeric
         * COALESCE(price,    0)::numeric)               AS revenue,
        0::numeric(18,2)                                  AS cost
    FROM raw.ozon_stock;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))

# ---------- CLI ----------
def run():
    parser = argparse.ArgumentParser(description="ETL: Ozon stock_on_warehouses → raw")
    parser.add_argument("--dry-run", action="store_true", help="Не писать в БД, только запросить и показать счетчик")
    args = parser.parse_args()

    cfg = load_config()
    engine = make_engine(cfg)
    bootstrap_raw(engine)
    bootstrap_mart(engine)  # создадим схему mart и таблицу, если их ещё нет


    log_id = start_log(engine, source="ozon", endpoint="/v2/analytics/stock_on_warehouses")
    try:
        print("🚀 Запуск ETL из Ozon API...")
        items = fetch_all_ozon_stock(cfg, page_limit=1000)
        print(f"Получено всего: {len(items)}")

        df_norm = normalize_stock(items)
        rows_raw = rows_norm = 0
        if not args.dry_run:
            rebuild_mart_from_raw(engine)


        if not args.dry_run:
            rows_raw = persist_raw(engine, items)
            rows_norm = persist_normalized(engine, df_norm)

        finish_log(engine, log_id, status="ok", rows=rows_norm, message=f"raw={rows_raw}, norm={rows_norm}")
        print(f"✅ Записано в raw: {rows_raw}, нормализовано: {rows_norm}")
    except Exception as e:
        finish_log(engine, log_id, status="error", rows=0, message=str(e))
        print(f"❌ Ошибка: {e}")
        raise

if __name__ == "__main__":
    run()
