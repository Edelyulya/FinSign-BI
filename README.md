# FinSign BI - BI-дашборд маркетплейсов

**FinSign BI** - локальное аналитическое приложение для визуализации продаж с маркетплейсов **Ozon** и **Wildberries**.

Технологии:
- Python 3.13  
- Streamlit  
- PostgreSQL (в Docker)  
- SQLAlchemy, Pandas  
- Plotly Express  
- Power BI

Проект предназначен для аналитики выручки, прибыли и маржинальности по дням и площадкам, с возможностью автоматического сбора данных через API.

---

## Основные функции

- **ETL загрузка данных из Ozon API и WB API**  
  Скрипты: `etl/ozon_loader.py` и `etl/wb_loader.py`.  
  Слой `raw.*` хранит исходные выгрузки, слой `mart.*` — агрегированные данные.

- **Streamlit Dashboard**  
  KPI (выручка, прибыль, маржа), графики по дням и площадкам, панель администратора для запуска ETL.

- **Логирование ETL**  
  В таблицу `raw.etl_log` записываются все загрузки и ошибки.

- **Поддержка PostgreSQL через Docker**

- **Интеграция с Power BI Desktop** (через подключение к PostgreSQL)

---

## Установка и запуск

### 1. Клонирование репозитория

`git clone https://github.com/Edelyulya/FinSign-BI.git`

### 2. Установка зависимостей

`python -m venv .venv
`source .venv/bin/activate`  - для macOS/Linux
или `.venv\Scripts\activate` - для Windows

`pip install -r requirements.txt`

### 3. Запуск PostgreSQL в Docker
`docker run --name pg-finsign -e POSTGRES_PASSWORD=12345 -p 5432:5432 -d postgres`

### 4. Создание базы и применение схем
`docker exec -it pg-finsign psql -U postgres -c "CREATE DATABASE finsign;"`

`docker cp sql/schema.sql pg-finsign:/schema.sql`

`docker exec -it pg-finsign psql -U postgres -d finsign -f /schema.sql`

### 5. Настройка файла config.json
`{`
  `"db": {`
    `"user": "postgres",`
    `"password": "xxx",`
    `"host": "xxx",`
    `"port": 5432,`
    `"database": "xxx"`
  `},`
  `"ozon_api": {`
    `"client_id": "XXXX",`
    `"api_key": "XXXX"`
  `},`
  `"wb_api": {`
    `"token": "XXXX"`
  `}`
`}`

Файл config.json не коммитится в репозиторий

### Запуск приложения
`streamlit run app/app.py`
Приложение будет доступно по адресу:
http://localhost:8501

### Админ-панель Streamlit
Слева в интерфейсе доступны кнопки управления:
- **Пересобрать витрину** - обновляет mart.fact_sales на основе данных raw.*
- **Запустить ETL Ozon и пересобрать** - выгружает данные из Ozon API и пересобирает витрину
- **Запустить ETL WB и пересобрать** - выгружает данные из Wildberries API и пересобирает витрину (при наличии токена)

### Архитектура витрины
Слои данных:
- raw - исходные выгрузки API
- raw.ozon_stock
- raw.wb_sales
- mart - агрегированные данные
- mart.fact_sales - объединение данных с обеих площадок
- mart.vw_kpi - представление для Power BI и Streamlit`

### Логирование ETL
Логирование выполняется в таблицу raw.etl_log с полями:
- dt_start, dt_end - время начала и завершения
- source - источник данных (ozon, wb)
- status - ok / error
- rows_loaded — количество загруженных строк`

### Интеграция Power BI Desktop (Windows)
- Установите Power BI Desktop (Free)
- Подключитесь к локальной базе PostgreSQL:
Server: localhost
Database: xxx
User: postgres
Password: xxx
- Выберите таблицу mart.vw_kpi.
- Постройте отчёт с выручкой и прибылью по площадкам и датам.
- Сохраните .pbix как шаблон корпоративного отчёта.

## Автор
> Юлия Салодкина
> SDET Engineer, FinSign BI Project
> 📍 Москва / РФ
> 🔗 GitHub: Edelyulya
