#!/bin/bash
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname data_warehouse <<'SQL'
CREATE SCHEMA IF NOT EXISTS public AUTHORIZATION warehouse_user;

GRANT CONNECT ON DATABASE data_warehouse TO warehouse_user;
GRANT CONNECT ON DATABASE data_warehouse TO analytics_user;
GRANT USAGE ON SCHEMA public TO warehouse_user;
GRANT USAGE ON SCHEMA public TO analytics_user;

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id SERIAL PRIMARY KEY,
    external_customer_id VARCHAR(50) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(100),
    country VARCHAR(50),
    city VARCHAR(100),
    phone VARCHAR(20),
    registration_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_customers_email ON dim_customers(email);
CREATE INDEX IF NOT EXISTS idx_dim_customers_country ON dim_customers(country);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id SERIAL PRIMARY KEY,
    external_product_id VARCHAR(50) UNIQUE NOT NULL,
    product_name VARCHAR(200),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    unit_price DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_products_category ON dim_products(category);
CREATE INDEX IF NOT EXISTS idx_dim_products_brand ON dim_products(brand);

CREATE TABLE IF NOT EXISTS dim_dates (
    date_id INTEGER PRIMARY KEY,
    date DATE UNIQUE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day INTEGER,
    week INTEGER,
    day_name VARCHAR(10),
    month_name VARCHAR(10),
    is_weekend BOOLEAN
);

INSERT INTO dim_dates
SELECT
    CAST(TO_CHAR(d, 'YYYYMMDD') AS INTEGER),
    d,
    EXTRACT(YEAR FROM d),
    EXTRACT(QUARTER FROM d),
    EXTRACT(MONTH FROM d),
    EXTRACT(DAY FROM d),
    EXTRACT(WEEK FROM d),
    TO_CHAR(d, 'Day'),
    TO_CHAR(d, 'Month'),
    EXTRACT(DOW FROM d) IN (0, 6)
FROM generate_series(
    CURRENT_DATE - INTERVAL '5 years',
    CURRENT_DATE + INTERVAL '1 year',
    INTERVAL '1 day'
) AS d
ON CONFLICT (date_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id BIGSERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES dim_customers(customer_id),
    product_id INTEGER REFERENCES dim_products(product_id),
    date_id INTEGER REFERENCES dim_dates(date_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2),
    total_amount DECIMAL(12, 2),
    discount_percent DECIMAL(5, 2) DEFAULT 0,
    net_amount DECIMAL(12, 2),
    order_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON fact_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON fact_sales(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_order ON fact_sales(order_id);

CREATE TABLE IF NOT EXISTS agg_daily_sales (
    date_id INTEGER PRIMARY KEY REFERENCES dim_dates(date_id),
    total_sales DECIMAL(15, 2),
    total_quantity INTEGER,
    transaction_count INTEGER,
    unique_customers INTEGER,
    avg_transaction_value DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS agg_product_performance (
    product_id INTEGER PRIMARY KEY REFERENCES dim_products(product_id),
    total_sales DECIMAL(15, 2),
    total_quantity INTEGER,
    transaction_count INTEGER,
    unique_customers INTEGER,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS agg_customer_lifetime_value (
    customer_id INTEGER PRIMARY KEY REFERENCES dim_customers(customer_id),
    total_spent DECIMAL(15, 2),
    total_transactions INTEGER,
    avg_transaction_value DECIMAL(12, 2),
    last_purchase_date DATE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data_quality_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    metric_type VARCHAR(50),
    metric_name VARCHAR(200),
    metric_value DECIMAL(15, 4),
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_execution_log (
    execution_id BIGSERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100),
    stage VARCHAR(50),
    status VARCHAR(20),
    rows_processed INTEGER,
    rows_failed INTEGER,
    execution_start TIMESTAMP,
    execution_end TIMESTAMP,
    duration_seconds INTEGER,
    error_message TEXT
);

CREATE OR REPLACE VIEW v_sales_summary AS
SELECT
    d.date,
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT fs.sale_id) AS transaction_count,
    COUNT(DISTINCT fs.customer_id) AS unique_customers,
    SUM(fs.total_amount) AS total_sales,
    AVG(fs.total_amount) AS avg_transaction_value
FROM fact_sales fs
JOIN dim_dates d ON fs.date_id = d.date_id
GROUP BY d.date_id, d.date, d.year, d.month, d.month_name;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO warehouse_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO warehouse_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analytics_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO warehouse_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO warehouse_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO analytics_user;
SQL
