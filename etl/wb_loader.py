# etl/wb_loader.py
import argparse
import json
import time
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Any

import pandas as pd
import requests
from sqlalchemy import create_engine, text

WB_URL_REPORT = "https://statistics-api.wildberries.ru/api/v1/supplier/reportDetailByPeriod"
DEFAULT_LIMIT = 100000  # максимально разрешённый лимит на вызов

# ---------- utils ----------

def load_cfg() -> Dict[str, Any]:
    """Читает конфиг из config.json в корне репо."""
    ROOT = Path(__file__).resolve().parents[1]
    cfg_path = ROOT / "config.json"
    with cfg_path.open("r", encoding="utf-8") as f:
        return json.load(f)

def make_engine(cfg: Dict[str, Any]):
    db = cfg["db"]
    conn = (
        f"postgresql+psycopg://{db['user']}:{db['password']}"
        f"@{db['host']}:{db['port']}/{db['database']}"
    )
    return create_engine(conn, pool_pre_ping=True)

def coalesce(*vals, default=None):
    for v in vals:
        if v not in (None, "", "null"):
            return v
    return default

def parse_date(s: str) -> date | None:
    if not s:
        return None
    # WB может прислать "2025-10-29T11:22:33" или "2025-10-29 11:22:33"
    s = s.replace("T", " ")
    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").date()
        except Exception:
            return None

# ---------- WB API ----------

def fetch_report_batch(token: str, since: str, until: str, rrdid: int) -> List[Dict[str, Any]]:
    """
    Возвращает одну "страницу" отчёта продаж за период.
    Пагинация через rrdid (0 -> следующий rrdid из ответа).
    """
    headers = {"Authorization": token}
    payload = {
        "dateFrom": since,
        "dateTo": until,
        "rrdid": rrdid,
        "limit": DEFAULT_LIMIT,
    }
    r = requests.post(WB_URL_REPORT, headers=headers, json=payload, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"WB API HTTP {r.status_code}: {r.text}")
    # Ответ — JSON-массив строк отчёта
    return r.json()

def fetch_report_all(token: str, since: str, until: str) -> List[Dict[str, Any]]:
    """Забираем все страницы отчёта, пока не кончатся данные."""
    results: List[Dict[str, Any]] = []
    rrdid = 0
    page = 0
    while True:
        page += 1
        batch = fetch_report_batch(token, since, until, rrdid)
        if not batch:
            break
        results.extend(batch)
        # rrdid — это id последней записи; берём максимум из партии
        rrdid = max(item.get("rrd_id") or item.get("rrdid") or 0 for item in batch)
        # чуть-чуть паузы, на всякий случай
        time.sleep(0.2)
    return results

# ---------- transform & load ----------

def normalize_to_df(items: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Приводим к нашей "сырой" форме.
    Поля в WB могут называться по-разному в разных ревизиях отчёта,
    поэтому берём с запасом: sale_dt/saleDt, supplierArticle/sa_article/sa_name и т.п.
    """
    rows = []
    for it in items:
        sale_dt = coalesce(it.get("sale_dt"), it.get("saleDt"), it.get("date"))
        dt = parse_date(sale_dt)

        sku = coalesce(
            it.get("supplierArticle"),
            it.get("sa_article"),
            it.get("sa_name"),
            it.get("nm_id"),
            it.get("barcode"),
            default="UNKNOWN",
        )

        region = coalesce(it.get("regionName"), it.get("region_name"))

        qty = coalesce(it.get("quantity"), it.get("sale_qty"), 0) or 0
        # цена/выручка: retail_price — цена, retail_amount — сумма по строке
        price = coalesce(it.get("retail_price"), it.get("price"), 0) or 0
        # В raw мы храним qty/price, для mart посчитаем revenue = qty*price
        rows.append(
            {
                "date": dt,
                "sku": str(sku) if sku is not None else "UNKNOWN",
                "region": region,
                "quantity": float(qty),
                "price": float(price),
                "payload": it,
            }
        )
    df = pd.DataFrame(rows, columns=["date", "sku", "region", "quantity", "price", "payload"])
    # уберём явный мусор
    if not df.empty:
        df = df[df["date"].notna()]
    return df

def upsert_raw_wb(engine, df: pd.DataFrame) -> int:
    """
    Простая стратегия: очищаем окно и кладём свежие данные (idempotent для периода).
    Тут мы удалим старые WB-записи за период и перезапишем.
    """
    if df.empty:
        return 0
    d_min = df["date"].min()
    d_max = df["date"].max()
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM raw.wb_sales WHERE date BETWEEN :dmin AND :dmax"),
            {"dmin": d_min, "dmax": d_max},
        )
    df.to_sql("wb_sales", engine, schema="raw", if_exists="append", index=False, method="multi", chunksize=1000)
    return len(df)

def log_etl(engine, status: str, source: str, rows: int, meta: Dict[str, Any] | None = None):
    """
    Пишем лог, если есть таблица raw.etl_log (не обязательно для первого запуска).
    """
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO raw.etl_log(run_at, source, status, rows, meta)
                    VALUES (now(), :source, :status, :rows, CAST(:meta AS jsonb))
                """),
                {"source": source, "status": status, "rows": rows, "meta": json.dumps(meta or {})},
            )
    except Exception:
        # тихо игнорируем, если таблицы ещё нет
        pass

# ---------- CLI ----------

def run():
    parser = argparse.ArgumentParser(description="WB → raw.wb_sales loader")
    parser.add_argument("--since", required=True, help="Дата начала периода (YYYY-MM-DD)")
    parser.add_argument("--until", required=True, help="Дата конца периода (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Не писать в БД, только показать объём")
    args = parser.parse_args()

    cfg = load_cfg()
    token = cfg["wb_api"]["token"]
    engine = make_engine(cfg)

    print("🚀 Запуск ETL из WB API...")
    items = fetch_report_all(token, args.since, args.until)
    print(f"• Получено строк: {len(items)}")

    df = normalize_to_df(items)
    print(f"• Нормализовано строк: {len(df)}")

    if args.dry_run:
        print("ℹ️ DRY-RUN: запись в БД пропущена.")
        return

    written = upsert_raw_wb(engine, df)
    log_etl(engine, status="ok", source="wb", rows=written, meta={"since": args.since, "until": args.until})
    print(f"✅ Записано в raw.wb_sales: {written}")

if __name__ == "__main__":
    run()
