#!/bin/bash

# Data Platform Startup Script
# =============================
# Initializes MinIO buckets and starts the data platform

set -e

echo "=========================================="
echo "🚀 Data Platform Startup Script"
echo "=========================================="

# Function to wait for service
wait_for_service() {
    local host=$1
    local port=$2
    local max_attempts=30
    local attempt=1
    
    echo "⏳ Waiting for $host:$port..."
    while ! nc -z "$host" "$port" 2>/dev/null; do
        if [ $attempt -eq $max_attempts ]; then
            echo "❌ Timeout waiting for $host:$port"
            exit 1
        fi
        echo "   Attempt $attempt/$max_attempts..."
        sleep 2
        attempt=$((attempt + 1))
    done
    echo "✅ $host:$port is ready"
}

COMPOSE_FILES="--env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml"

echo "🐳 Starting Docker Compose services..."
docker compose $COMPOSE_FILES up -d

# Wait for critical services
echo ""
echo "⏳ Waiting for services to be ready..."
wait_for_service localhost 9000  # MinIO
wait_for_service localhost 5432  # PostgreSQL
wait_for_service localhost 8080  # Airflow
wait_for_service localhost 7077  # Spark Master

# Initialize MinIO buckets
echo ""
echo "📦 Initializing MinIO buckets..."

# Create buckets using mc (MinIO client)
docker compose $COMPOSE_FILES exec -T minio mc alias set minio http://minio:9000 minioadmin minioadmin_dev_pass
docker compose $COMPOSE_FILES exec -T minio mc mb minio/bronze || true
docker compose $COMPOSE_FILES exec -T minio mc mb minio/silver || true
docker compose $COMPOSE_FILES exec -T minio mc mb minio/gold || true

echo "✅ MinIO buckets initialized"

# Generate test data
echo ""
echo "📊 Generating test data..."
docker compose $COMPOSE_FILES exec -T airflow-web python /opt/airflow/scripts/generate_data.py

echo ""
echo "=========================================="
echo "✅ Data Platform is Ready!"
echo "=========================================="
echo ""
echo "📊 Access Points:"
echo "   - Airflow UI:     http://localhost:8080 (admin/admin123)"
echo "   - MinIO Console:  http://localhost:9001 (minioadmin/minioadmin_dev_pass)"
echo "   - Spark Master:   http://localhost:8081"
echo "   - PostgreSQL:     localhost:5432 (warehouse_user/warehouse_dev_pass)"
echo ""
echo "🚀 Next steps:"
echo "   1. Access Airflow at http://localhost:8080"
echo "   2. Enable the 'data_platform_medallion_pipeline' DAG"
echo "   3. Trigger a manual run or wait for the scheduled time"
echo "=========================================="
