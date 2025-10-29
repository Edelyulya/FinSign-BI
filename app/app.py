import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# ---------- Config ----------
ROOT = Path(__file__).resolve().parents[1]          # корень репо
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
    # Лёгкая проверка коннекта (не обязательна, но полезна)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
except OperationalError as e:
    st.error("❌ Не удалось подключиться к PostgreSQL.")
    st.code(str(e))
    st.stop()
except Exception as e:
    st.error("❌ Непредвиденная ошибка при создании подключения.")
    st.code(str(e))
    st.stop()

# ---------- Load data ----------
try:
    df = pd.read_sql("SELECT * FROM mart.fact_sales", engine)
except Exception as e:
    st.warning("⚠️ Таблица mart.fact_sales недоступна или пуста.")
    st.code(str(e))
    st.stop()

if df.empty:
    st.info("ℹ️ В таблице mart.fact_sales пока нет данных.")
    st.stop()

# ---------- KPIs ----------
col1, col2, col3 = st.columns(3)
revenue_sum = float(df["revenue"].sum() or 0)
profit_sum = float(df["profit"].sum() or 0)
margin = (profit_sum / revenue_sum * 100) if revenue_sum else 0.0

col1.metric("Выручка", f"{revenue_sum:,.0f} ₽")
col2.metric("Прибыль", f"{profit_sum:,.0f} ₽")
col3.metric("Средняя маржа", f"{margin:.1f}%")

# ---------- Charts ----------
fig = px.bar(
    df,
    x="date",
    y="revenue",
    color="marketplace",
    title="Выручка по дням",
    labels={"date": "Дата", "revenue": "Выручка (₽)", "marketplace": "Площадка"},
)
st.plotly_chart(fig, use_container_width=True)
