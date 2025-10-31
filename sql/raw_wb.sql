-- raw слой для WB
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.wb_sales (
    id         bigserial PRIMARY KEY,
    dt         timestamptz DEFAULT now(),

    -- нормализованные поля
    date       date        NOT NULL,
    sku        text        NOT NULL,
    region     text,
    quantity   numeric(18,2) NOT NULL DEFAULT 0,
    price      numeric(18,2) NOT NULL DEFAULT 0
);

-- вспомогательные индексы
CREATE INDEX IF NOT EXISTS ix_wb_sales_date   ON raw.wb_sales(date);
CREATE INDEX IF NOT EXISTS ix_wb_sales_sku    ON raw.wb_sales(sku);
CREATE INDEX IF NOT EXISTS ix_wb_sales_region ON raw.wb_sales(region);
