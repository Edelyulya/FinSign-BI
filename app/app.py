import json
import subprocess
from pathlib import Path
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# ---------- helpers (DDL) ----------
def ensure_mart(engine):
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


# ---------- Config ----------
ROOT = Path(__file__).resolve().parents[1]     # корень репо
CONFIG_PATH = ROOT / "config.json"

st.set_page_config(page_title="FinSign BI", layout="wide")
st.title("📊 FinSign BI Dashboard")

if not CONFIG_PATH.exists():
    st.error("❌ Файл config.json не найден в корне проекта.")
    st.stop()

with CONFIG_PATH.open("r", encoding="utf-8") as f:
    cfg = json.load(f)

db = cfg["db"]

# ---------- DB engine (psycopg v3) ----------
conn_str = (
    f"postgresql+psycopg://{db['user']}:{db['password']}"
    f"@{db['host']}:{db['port']}/{db['database']}"
)
try:
    engine = create_engine(conn_str, pool_pre_ping=True)
    ensure_mart(engine)  # гарантируем наличие витрины

    # лёгкая проверка коннекта
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
except OperationalError as e:
    st.error("❌ Не удалось подключиться к PostgreSQL.")
    st.code(str(e))
    st.stop()

# ---------- Sidebar: admin actions ----------
st.sidebar.header("Админ-панель")

# кнопка: просто пересобрать витрину из raw.*
if st.sidebar.button("⟳ Пересобрать витрину", use_container_width=True):
    try:
        rebuild_mart_from_raw(engine)
        st.success("✅ Витрина пересобрана из raw.*")
    except Exception as e:
        st.error("❌ Ошибка пересборки витрины")
        st.code(str(e))

# кнопка: запустить локальный ETL (python etl/ozon_loader.py) и затем пересобрать витрину
if st.sidebar.button("⇅ Запустить ETL Ozon и пересобрать", type="primary", use_container_width=True):
    try:
        # запускаем скрипт синхронно, чтобы дождаться завершения
        proc = subprocess.run(
            ["python", str(ROOT / "etl" / "ozon_loader.py")],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            check=False
        )
        if proc.returncode != 0:
            st.error("❌ ETL завершился с ошибкой")
            st.code(proc.stdout + "\n" + proc.stderr)
        else:
            st.success("✅ ETL Ozon выполнен")
            rebuild_mart_from_raw(engine)
            st.success("✅ Витрина пересобрана")
    except FileNotFoundError:
        st.error("❌ Не найден etl/ozon_loader.py")
    except Exception as e:
        st.error("❌ Не удалось запустить ETL")
        st.code(str(e))

st.sidebar.caption(f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ---------- Автопересборка при первом старте (без кнопки) ----------
try:
    rebuild_mart_from_raw(engine)
except Exception:
    # если raw ещё пустой — молча пропускаем
    pass

# ---------- Загрузка данных для дашборда ----------
try:
    df = pd.read_sql("SELECT * FROM mart.fact_sales ORDER BY date", engine)
except Exception as e:
    st.warning("⚠️ Не удалось загрузить данные из БД.")
    st.text(e)
    st.stop()

if df.empty:
    st.info("ℹ️ В таблице mart.fact_sales пока нет данных.")
    st.stop()

# ---------- KPI ----------
col1, col2, col3 = st.columns(3)
col1.metric("Выручка", f"{df['revenue'].sum():,.0f} ₽")
col2.metric("Прибыль", f"{(df['revenue'].sum() - df['cost'].sum()):,.0f} ₽")
margin = (df["revenue"].sum() - df["cost"].sum()) / df["revenue"].sum() * 100 if df["revenue"].sum() else 0
col3.metric("Средняя маржа", f"{margin:.1f}%")

# ---------- Chart ----------
fig = px.bar(
    df,
    x="date",
    y="revenue",
    color="marketplace",
    title="Выручка по дням",
    labels={"date": "Дата", "revenue": "Выручка (₽)", "marketplace": "Площадка"}
)
st.plotly_chart(fig, use_container_width=True)
