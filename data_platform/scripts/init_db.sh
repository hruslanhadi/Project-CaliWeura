#!/bin/bash
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<'SQL'
SELECT 'CREATE DATABASE airflow OWNER airflow_meta'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'airflow')
\gexec

SELECT 'CREATE DATABASE data_warehouse OWNER warehouse_user'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'data_warehouse')
\gexec
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<SQL
ALTER DATABASE airflow OWNER TO airflow_meta;
ALTER DATABASE data_warehouse OWNER TO warehouse_user;
SQL
