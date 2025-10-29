CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.fact_sales (
    id SERIAL PRIMARY KEY,
    date DATE,
    marketplace VARCHAR(20),
    region VARCHAR(50),
    revenue NUMERIC,
    cost NUMERIC,
    profit NUMERIC
);