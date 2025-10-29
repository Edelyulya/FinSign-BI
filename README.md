# FinSign-BI
FinSign BI - Аналитическая Платформа. Ввнутренняя BI-платформа для анализа финансовых и операционных данных.

## Стек
- Python 3.13+
- PostgreSQL (основная БД)
- Streamlit (дашборды)
- Pandas / SQLAlchemy / Plotly
- ETL: Python-скрипты (через Airflow или cron)

## Структура
app/ # Streamlit-интерфейс
etl/ # загрузка данных (API, CSV)
sql/ # схемы БД
config.json # настройки подключения


##  Быстрый старт
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app/app.py

*По умолчанию Streamlit доступен на http://localhost:8501.*

## Цель MVP
Загрузка данных из Ozon (API)
Запись в PostgreSQL
Визуализация через Streamlit
Две роли доступа (admin / manager)
