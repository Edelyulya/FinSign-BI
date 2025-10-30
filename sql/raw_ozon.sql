BEGIN;

-- Слой "бронза" для Ozon + логирование
CREATE SCHEMA IF NOT EXISTS raw;

-- Лог ETL
CREATE TABLE IF NOT EXISTS raw.etl_log (
  id           bigserial PRIMARY KEY,
  source       text        NOT NULL,                  -- 'ozon'
  endpoint     text        NOT NULL,                  -- '/v2/analytics/stock_on_warehouses'
  started_at   timestamptz NOT NULL DEFAULT now(),
  finished_at  timestamptz,
  status       text        NOT NULL DEFAULT 'running', -- running | ok | error
  rows_loaded  integer     DEFAULT 0,
  message      text
);

-- Сырой JSON-ответ (для аудита и перепарса)
CREATE TABLE IF NOT EXISTS raw.ozon_stock_raw (
  id         bigserial PRIMARY KEY,
  loaded_at  timestamptz NOT NULL DEFAULT now(),
  payload    jsonb       NOT NULL
);

-- Нормализованный слой (таблично)
CREATE TABLE IF NOT EXISTS raw.ozon_stock (
  loaded_at       timestamptz NOT NULL DEFAULT now(),
  date            date,
  warehouse_name  text,
  region          text,
  product_id      text,
  sku             text,
  item_name       text,
  quantity        numeric,
  reserved        numeric,
  price           numeric
);

CREATE INDEX IF NOT EXISTS ix_ozon_stock_date ON raw.ozon_stock(date);
CREATE INDEX IF NOT EXISTS ix_ozon_stock_sku  ON raw.ozon_stock(sku);

COMMIT;
