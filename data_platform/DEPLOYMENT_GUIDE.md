# =====================================================
# DEPLOYMENT QUICK REFERENCE
# =====================================================

## Development (16GB Laptop)

```bash
cd data_platform
cp .env.development .env
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d
# Wait 2-3 minutes
docker compose ps
# Open http://localhost:8080 (admin/admin123)
```

## Production

```bash
cd data_platform
cp .env.production .env
# EDIT .env with actual passwords
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
docker compose logs -f airflow-init
# Wait for "service_completed_successfully"
docker compose ps
```

## Useful Commands

```bash
# Development logs
docker compose -f docker-compose.yml -f docker-compose.dev.yml logs -f airflow-scheduler

# Production logs
docker compose -f docker-compose.yml -f docker-compose.prod.yml logs -f airflow-init

# Stop services
docker compose -f docker-compose.yml -f docker-compose.dev.yml down
docker compose -f docker-compose.yml -f docker-compose.prod.yml down

# Check health
./scripts/health_check.sh

# Database access
docker compose exec postgres psql -U airflow_meta -d airflow

# MinIO browser
# http://localhost:9001 (minioadmin/password)

# Spark UI
# http://localhost:8081
```

## Troubleshooting

```bash
# Service logs
docker compose logs <service_name>

# Resource usage
docker stats

# Clean up
docker system prune
docker volume prune

# Force recreate
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --force-recreate
```

## Security Checklist

- [ ] Change admin password: `docker compose exec airflow-web airflow users create --username admin --role Admin --password newpass`
- [ ] Update .env credentials
- [ ] Enable RBAC
- [ ] Configure TLS/SSL
- [ ] Review network policies
- [ ] Set up monitoring
- [ ] Enable audit logging
- [ ] Test backup/recovery

For detailed information, see [RUNBOOKS.md](RUNBOOKS.md)
