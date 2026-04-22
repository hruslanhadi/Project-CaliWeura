-- ============================================================================
-- Data Warehouse Schema Initialization
-- ============================================================================

-- Create warehouse user and database
CREATE USER warehouse_user WITH PASSWORD 'warehouse_pass';
CREATE DATABASE data_warehouse OWNER warehouse_user;

-- Connect to warehouse database
\c data_warehouse warehouse_user;

-- ============================================================================
-- DIMENSION TABLES (Star Schema)
-- ============================================================================

-- Customers Dimension
CREATE TABLE dim_customers (
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

CREATE INDEX idx_dim_customers_email ON dim_customers(email);
CREATE INDEX idx_dim_customers_country ON dim_customers(country);

-- Products Dimension
CREATE TABLE dim_products (
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

CREATE INDEX idx_dim_products_category ON dim_products(category);
CREATE INDEX idx_dim_products_brand ON dim_products(brand);

-- Date Dimension
CREATE TABLE dim_dates (
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

-- Populate date dimension (5 years)
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
) AS d;

-- ============================================================================
-- FACT TABLES
-- ============================================================================

-- Sales Fact Table
CREATE TABLE fact_sales (
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

CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_id);
CREATE INDEX idx_fact_sales_date ON fact_sales(date_id);
CREATE INDEX idx_fact_sales_order ON fact_sales(order_id);

-- ============================================================================
-- AGGREGATION TABLES (Pre-computed)
-- ============================================================================

-- Daily Sales Summary
CREATE TABLE agg_daily_sales (
    date_id INTEGER REFERENCES dim_dates(date_id),
    total_sales DECIMAL(15, 2),
    total_quantity INTEGER,
    transaction_count INTEGER,
    unique_customers INTEGER,
    avg_transaction_value DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_id)
);

-- Product Performance
CREATE TABLE agg_product_performance (
    product_id INTEGER REFERENCES dim_products(product_id),
    total_sales DECIMAL(15, 2),
    total_quantity INTEGER,
    transaction_count INTEGER,
    unique_customers INTEGER,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id)
);

-- Customer Lifetime Value
CREATE TABLE agg_customer_lifetime_value (
    customer_id INTEGER REFERENCES dim_customers(customer_id),
    total_spent DECIMAL(15, 2),
    total_transactions INTEGER,
    avg_transaction_value DECIMAL(12, 2),
    last_purchase_date DATE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer_id)
);

-- ============================================================================
-- METADATA TABLES
-- ============================================================================

-- Data Quality Metrics
CREATE TABLE data_quality_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    metric_type VARCHAR(50),
    metric_name VARCHAR(200),
    metric_value DECIMAL(15, 4),
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Pipeline Execution Log
CREATE TABLE pipeline_execution_log (
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

-- Grant permissions
GRANT CONNECT ON DATABASE data_warehouse TO warehouse_user;
GRANT USAGE ON SCHEMA public TO warehouse_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO warehouse_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO warehouse_user;

-- Create views
CREATE VIEW v_sales_summary AS
SELECT 
    d.date,
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT fs.sale_id) as transaction_count,
    COUNT(DISTINCT fs.customer_id) as unique_customers,
    SUM(fs.total_amount) as total_sales,
    AVG(fs.total_amount) as avg_transaction_value
FROM fact_sales fs
JOIN dim_dates d ON fs.date_id = d.date_id
GROUP BY d.date_id, d.date, d.year, d.month, d.month_name;

-- Set up audit columns for all tables
ALTER TABLE dim_customers ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_products ENABLE ROW LEVEL SECURITY;
ALTER TABLE fact_sales ENABLE ROW LEVEL SECURITY;
