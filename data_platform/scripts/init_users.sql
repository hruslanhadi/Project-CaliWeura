-- =====================================================
-- PostgreSQL User Management and Permissions
-- =====================================================
-- Creates separate users with minimal required privileges
-- Principle: Least privilege access

\set ON_ERROR_STOP on

-- =====================================================
-- CREATE USERS WITH SECURE PASSWORDS
-- =====================================================

-- Airflow metadata user (full access to airflow database)
CREATE USER IF NOT EXISTS airflow_meta WITH PASSWORD :'POSTGRES_PASSWORD';
ALTER USER airflow_meta WITH ENCRYPTED PASSWORD :'POSTGRES_PASSWORD';

-- Data warehouse user (all access to data warehouse)
CREATE USER IF NOT EXISTS warehouse_user WITH PASSWORD :'PG_WAREHOUSE_PASSWORD';
ALTER USER warehouse_user WITH ENCRYPTED PASSWORD :'PG_WAREHOUSE_PASSWORD';

-- Analytics user (read-only to data warehouse)
CREATE USER IF NOT EXISTS analytics_user WITH PASSWORD :'PG_ANALYTICS_PASSWORD';
ALTER USER analytics_user WITH ENCRYPTED PASSWORD :'PG_ANALYTICS_PASSWORD';

-- =====================================================
-- AIRFLOW METADATA USER PERMISSIONS
-- =====================================================

-- Grant full access to airflow database
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow_meta;

-- Make airflow_meta the owner of airflow database schema
GRANT ALL PRIVILEGES ON SCHEMA public TO airflow_meta;

-- Airflow needs to create tables during initialization
ALTER DEFAULT PRIVILEGES IN DATABASE airflow GRANT ALL ON TABLES TO airflow_meta;
ALTER DEFAULT PRIVILEGES IN DATABASE airflow GRANT ALL ON SEQUENCES TO airflow_meta;

-- =====================================================
-- WAREHOUSE USER PERMISSIONS (on airflow database)
-- =====================================================

-- Warehouse user needs basic connect permission
GRANT CONNECT ON DATABASE airflow TO warehouse_user;
GRANT USAGE ON SCHEMA public TO warehouse_user;

-- Read-only access to airflow tables (doesn't write to airflow db)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO warehouse_user;

-- =====================================================
-- ANALYTICS USER PERMISSIONS (on airflow database)
-- =====================================================

-- Analytics user can connect and read
GRANT CONNECT ON DATABASE airflow TO analytics_user;
GRANT USAGE ON SCHEMA public TO analytics_user;

-- Read-only access to selected tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analytics_user;

-- =====================================================
-- SECURITY SETTINGS
-- =====================================================

-- Disable superuser privileges (principle of least privilege)
ALTER USER airflow_meta NOSUPERUSER;
ALTER USER warehouse_user NOSUPERUSER;
ALTER USER analytics_user NOSUPERUSER;

-- Set password validity (in days) - 90 days for development, 60 for production
ALTER USER airflow_meta VALID UNTIL '2027-04-23';
ALTER USER warehouse_user VALID UNTIL '2027-04-23';
ALTER USER analytics_user VALID UNTIL '2027-04-23';

-- =====================================================
-- ENABLE CONNECTION LOGGING
-- =====================================================

-- These settings are applied globally, but document what should be configured:
-- In postgresql.conf:
-- - log_connections = on
-- - log_disconnections = on
-- - log_statement = 'all'  (for debug mode only)
-- - log_duration = on

-- =====================================================
-- END OF USER INITIALIZATION
-- =====================================================
