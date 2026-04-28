#!/bin/bash
set -euo pipefail

psql \
  -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname postgres <<SQL
DO
\$\$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow_meta') THEN
        CREATE ROLE airflow_meta LOGIN PASSWORD '${POSTGRES_PASSWORD}';
    ELSE
        ALTER ROLE airflow_meta WITH PASSWORD '${POSTGRES_PASSWORD}';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'warehouse_user') THEN
        CREATE ROLE warehouse_user LOGIN PASSWORD '${PG_WAREHOUSE_PASSWORD}';
    ELSE
        ALTER ROLE warehouse_user WITH PASSWORD '${PG_WAREHOUSE_PASSWORD}';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analytics_user') THEN
        CREATE ROLE analytics_user LOGIN PASSWORD '${PG_ANALYTICS_PASSWORD}';
    ELSE
        ALTER ROLE analytics_user WITH PASSWORD '${PG_ANALYTICS_PASSWORD}';
    END IF;
END
\$\$;

ALTER ROLE airflow_meta NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT;
ALTER ROLE warehouse_user NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT;
ALTER ROLE analytics_user NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT;
SQL