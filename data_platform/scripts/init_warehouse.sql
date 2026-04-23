-- =====================================================
-- PostgreSQL Initialization - Data Warehouse Schema
-- =====================================================
-- This script creates the data warehouse database and schemas
-- Used by the medallion architecture (bronze/silver/gold)

\set ON_ERROR_STOP on

-- Connect to postgres database
\c postgres

-- =====================================================
-- CREATE DATABASE
-- =====================================================
CREATE DATABASE data_warehouse
    ENCODING 'UTF8'
    TEMPLATE template0
    LC_COLLATE 'C'
    LC_CTYPE 'C';

-- =====================================================
-- CONNECT TO DATA WAREHOUSE AND CREATE SCHEMAS
-- =====================================================
\c data_warehouse

-- Bronze Schema (raw data ingestion)
CREATE SCHEMA IF NOT EXISTS bronze
    AUTHORIZATION postgres;
COMMENT ON SCHEMA bronze IS 'Raw data ingestion layer - untransformed data from source systems';

-- Silver Schema (cleaned and transformed data)
CREATE SCHEMA IF NOT EXISTS silver
    AUTHORIZATION postgres;
COMMENT ON SCHEMA silver IS 'Cleaned and transformed data - deduplicated, validated, business-conformed';

-- Gold Schema (aggregated business tables)
CREATE SCHEMA IF NOT EXISTS gold
    AUTHORIZATION postgres;
COMMENT ON SCHEMA gold IS 'Aggregated business tables - fact and dimension tables for analytics';

-- Staging Schema (temporary tables)
CREATE SCHEMA IF NOT EXISTS staging
    AUTHORIZATION postgres;
COMMENT ON SCHEMA staging IS 'Temporary staging area for intermediate transformations';

-- Audit Schema (data quality and audit logs)
CREATE SCHEMA IF NOT EXISTS audit
    AUTHORIZATION postgres;
COMMENT ON SCHEMA audit IS 'Data quality checks and audit logs';

-- =====================================================
-- CREATE BASE TABLES FOR EACH LAYER
-- =====================================================

-- BRONZE: Raw customer data
CREATE TABLE IF NOT EXISTS bronze.customers (
    _id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    phone VARCHAR(20),
    date_of_birth DATE,
    ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50),
    UNIQUE(customer_id, ingestion_date)
)
PARTITION BY RANGE (ingestion_date);
COMMENT ON TABLE bronze.customers IS 'Raw customer data ingested from source systems';

-- BRONZE: Raw order data
CREATE TABLE IF NOT EXISTS bronze.orders (
    _id SERIAL PRIMARY KEY,
    order_id INTEGER,
    customer_id INTEGER,
    order_date DATE,
    total_amount NUMERIC(10, 2),
    currency VARCHAR(3),
    order_status VARCHAR(50),
    ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50),
    UNIQUE(order_id, ingestion_date)
);
COMMENT ON TABLE bronze.orders IS 'Raw order data ingested from source systems';

-- BRONZE: Raw product data
CREATE TABLE IF NOT EXISTS bronze.products (
    _id SERIAL PRIMARY KEY,
    product_id INTEGER,
    product_name VARCHAR(255),
    category VARCHAR(100),
    price NUMERIC(10, 2),
    currency VARCHAR(3),
    stock_quantity INTEGER,
    ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50),
    UNIQUE(product_id, ingestion_date)
);
COMMENT ON TABLE bronze.products IS 'Raw product data ingested from source systems';

-- =====================================================
-- CREATE AUDIT TABLES
-- =====================================================

CREATE TABLE IF NOT EXISTS audit.data_quality_checks (
    check_id SERIAL PRIMARY KEY,
    table_name VARCHAR(255),
    check_name VARCHAR(255),
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    passed BOOLEAN,
    error_message TEXT,
    row_count INTEGER,
    duplicate_count INTEGER
);
COMMENT ON TABLE audit.data_quality_checks IS 'Data quality check results and logs';

CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(255),
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(50),
    record_count INTEGER,
    error_message TEXT
);
COMMENT ON TABLE audit.pipeline_runs IS 'Pipeline execution logs and metrics';

-- =====================================================
-- INDEXES
-- =====================================================

CREATE INDEX IF NOT EXISTS idx_bronze_customers_customer_id ON bronze.customers(customer_id);
CREATE INDEX IF NOT EXISTS idx_bronze_customers_ingestion_date ON bronze.customers(ingestion_date);
CREATE INDEX IF NOT EXISTS idx_bronze_orders_customer_id ON bronze.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_bronze_orders_order_date ON bronze.orders(order_date);
CREATE INDEX IF NOT EXISTS idx_bronze_products_product_id ON bronze.products(product_id);

-- =====================================================
-- SET PERMISSIONS
-- =====================================================

-- Warehouse user gets full permissions on all schemas
GRANT ALL PRIVILEGES ON DATABASE data_warehouse TO warehouse_user;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE data_warehouse TO warehouse_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE data_warehouse TO warehouse_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN DATABASE data_warehouse TO warehouse_user;

-- Analytics user gets read-only permissions
GRANT CONNECT ON DATABASE data_warehouse TO analytics_user;
GRANT USAGE ON ALL SCHEMAS IN DATABASE data_warehouse TO analytics_user;
GRANT SELECT ON ALL TABLES IN DATABASE data_warehouse TO analytics_user;

-- Default privileges for future objects
ALTER DEFAULT PRIVILEGES IN DATABASE data_warehouse GRANT ALL ON SCHEMAS TO warehouse_user;
ALTER DEFAULT PRIVILEGES IN DATABASE data_warehouse GRANT ALL ON TABLES TO warehouse_user;
ALTER DEFAULT PRIVILEGES IN DATABASE data_warehouse GRANT SELECT ON TABLES TO analytics_user;

-- =====================================================
-- END OF INITIALIZATION
-- =====================================================
