#!/bin/bash
# Initialize Airflow Connections for the Data Platform

set -euo pipefail

echo "Initializing Airflow connections..."

# Wait for Airflow to be ready
max_attempts=30
attempt=1
while ! airflow connections list > /dev/null 2>&1; do
    if [ $attempt -eq $max_attempts ]; then
        echo "❌ Airflow failed to become ready"
        exit 1
    fi
    echo "Waiting for Airflow to be ready... (attempt $attempt/$max_attempts)"
    sleep 2
    attempt=$((attempt + 1))
done

echo "✅ Airflow is ready"

# Add Spark connection
echo "Adding Spark connection..."
airflow connections add spark_default \
    --conn-type spark \
    --conn-host spark-master \
    --conn-port 7077 \
    || echo "⚠️  Spark connection may already exist or failed"

# Add MinIO connection
echo "Adding MinIO connection..."
airflow connections add minio_default \
    --conn-type s3 \
    --conn-host minio \
    --conn-port 9000 \
    --conn-login minioadmin \
    --conn-password minioadmin_dev_pass \
    || echo "⚠️  MinIO connection may already exist or failed"

# Add PostgreSQL warehouse connection
echo "Adding PostgreSQL warehouse connection..."
airflow connections add postgres_warehouse \
    --conn-type postgres \
    --conn-host postgres \
    --conn-port 5432 \
    --conn-login warehouse_user \
    --conn-password warehouse_dev_pass \
    --conn-schema data_warehouse \
    || echo "⚠️  Postgres connection may already exist or failed"

echo "✅ Airflow connections initialized"
