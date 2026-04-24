#!/bin/bash

# Health Check Script
# ====================
# Monitors and logs the health of all data platform services

set -e
COMPOSE_FILES=${COMPOSE_FILES:-"--env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml"}

TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

echo "[$TIMESTAMP] 🏥 Data Platform Health Check"

# Check Docker services
echo ""
echo "🐳 Docker Services Status:"
docker compose $COMPOSE_FILES ps

# Check Airflow
echo ""
echo "🔄 Airflow Health:"
curl -s http://localhost:8080/health || echo "❌ Airflow unavailable"

# Check Spark
echo ""
echo "⚡ Spark Master:"
curl -s http://localhost:8081 | grep -q "spark" && echo "✅ Spark Master healthy" || echo "❌ Spark Master unavailable"

# Check MinIO
echo ""
echo "📦 MinIO Health:"
curl -s http://localhost:9000/minio/health/live | grep -q "Alive" && echo "✅ MinIO healthy" || echo "❌ MinIO unavailable"

# Check PostgreSQL
echo ""
echo "🗄️  PostgreSQL Health:"
docker compose $COMPOSE_FILES exec -T postgres pg_isready -U postgres && echo "✅ PostgreSQL healthy" || echo "❌ PostgreSQL unavailable"

# Check Redis
echo ""
echo "💾 Redis Health:"
docker compose $COMPOSE_FILES ps redis >/dev/null 2>&1 && docker compose $COMPOSE_FILES exec -T redis redis-cli ping && echo "✅ Redis healthy" || echo "ℹ️ Redis not enabled"

# Check Kafka
echo ""
echo "📨 Kafka Health:"
docker compose $COMPOSE_FILES ps kafka >/dev/null 2>&1 && docker compose $COMPOSE_FILES exec -T kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1 && echo "✅ Kafka healthy" || echo "ℹ️ Kafka not enabled"

echo ""
echo "✅ Health check completed"
