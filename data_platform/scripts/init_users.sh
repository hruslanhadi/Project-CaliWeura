#!/bin/bash
set -euo pipefail

psql \
  -v ON_ERROR_STOP=1 \
  -v postgres_password="$POSTGRES_PASSWORD" \
  -v warehouse_password="$PG_WAREHOUSE_PASSWORD" \
  -v analytics_password="$PG_ANALYTICS_PASSWORD" \
  --username "$POSTGRES_USER" \
  --dbname postgres <<'SQL'
DO
\$\$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow_meta') THEN
        CREATE ROLE airflow_meta LOGIN PASSWORD :'postgres_password';
    ELSE
        ALTER ROLE airflow_meta WITH PASSWORD :'postgres_password';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'warehouse_user') THEN
        CREATE ROLE warehouse_user LOGIN PASSWORD :'warehouse_password';
    ELSE
        ALTER ROLE warehouse_user WITH PASSWORD :'warehouse_password';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analytics_user') THEN
        CREATE ROLE analytics_user LOGIN PASSWORD :'analytics_password';
    ELSE
        ALTER ROLE analytics_user WITH PASSWORD :'analytics_password';
    END IF;
END
\$\$;

ALTER ROLE airflow_meta NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT;
ALTER ROLE warehouse_user NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT;
ALTER ROLE analytics_user NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT;
SQL
