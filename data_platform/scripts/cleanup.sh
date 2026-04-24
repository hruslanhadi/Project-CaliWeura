#!/bin/bash

# Cleanup Script
# ==============
# Stops and removes all containers, volumes (optional)

set -e
COMPOSE_FILES=${COMPOSE_FILES:-"--env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml"}

echo "=========================================="
echo "🧹 Data Platform Cleanup"
echo "=========================================="

read -p "Remove volumes and persistent data? (y/n) " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🗑️  Removing containers and volumes..."
    docker compose $COMPOSE_FILES down -v
    echo "✅ Containers and volumes removed"
else
    echo "🛑 Removing containers only (keeping data)..."
    docker compose $COMPOSE_FILES down
    echo "✅ Containers removed"
fi

echo "=========================================="
echo "✅ Cleanup completed"
echo "=========================================="
