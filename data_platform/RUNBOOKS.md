# =====================================================
# RUNBOOKS AND OPERATIONAL PROCEDURES
# =====================================================

# Table of Contents
1. [Quick Start](#quick-start)
2. [Development Deployment](#development-deployment)
3. [Production Deployment](#production-deployment)
4. [Troubleshooting](#troubleshooting)
5. [Maintenance](#maintenance)
6. [Security](#security)
7. [Monitoring](#monitoring)
8. [Backup & Recovery](#backup--recovery)

---

## Quick Start

### Prerequisites
- Docker & Docker Compose installed
- 16GB RAM minimum for development
- 32GB RAM+ for production
- 50GB free disk space

### Development Setup (16GB Laptop)

```bash
# 1. Navigate to project
cd data_platform

# 2. Copy development environment
cp .env.development .env

# 3. Make scripts executable
chmod +x scripts/*.sh

# 4. Start services
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d

# 5. Wait for initialization (2-3 minutes)
sleep 120

# 6. Check status
docker-compose ps

# 7. Access Airflow
# URL: http://localhost:8080
# Login: admin / admin123

# 8. View logs
docker-compose logs -f airflow-scheduler
```

### Production Setup

```bash
# 1. Navigate to project
cd data_platform

# 2. Copy production environment and update passwords
cp .env.production .env
# Edit .env with actual passwords and settings

# 3. Validate environment
docker-compose -f docker-compose.yml -f docker-compose.prod.yml config

# 4. Start services
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# 5. Monitor initialization
docker-compose logs -f airflow-init

# 6. Verify all services running
docker-compose ps
```

---

## Development Deployment

### Environment Variables (16GB Laptop)

```bash
# .env.development - Resource constraints for laptop
SPARK_EXECUTOR_MEMORY=512m      # Minimal
SPARK_DRIVER_MEMORY=512m
SPARK_EXECUTOR_CORES=1
AIRFLOW__CORE__EXECUTOR=LocalExecutor  # Single machine
```

### Commands

```bash
# Start all services
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d

# Stop services
docker-compose -f docker-compose.yml -f docker-compose.dev.yml down

# Rebuild after code changes
docker-compose -f docker-compose.yml -f docker-compose.dev.yml build

# View logs
docker-compose -f docker-compose.yml -f docker-compose.dev.yml logs -f airflow-scheduler

# Scale services (not recommended for dev)
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d --scale airflow-worker=1
```

### Testing

```bash
# Run health checks
./scripts/health_check.sh

# Generate test data
docker-compose exec airflow-web python scripts/generate_data.py

# Run integration tests
docker-compose exec airflow-web pytest tests/test_integration.py

# Run data quality tests
docker-compose exec airflow-web python tests/validate_data_quality.py
```

---

## Production Deployment

### Environment Configuration

```bash
# .env.production - High performance
SPARK_EXECUTOR_MEMORY=4g        # Higher for processing
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_CORES=4
SPARK_EXECUTOR_INSTANCES=4
AIRFLOW__CORE__EXECUTOR=CeleryExecutor  # Distributed execution
```

### Pre-Deployment Checklist

- [ ] Update all passwords in .env.production
- [ ] Generate FERNET_KEY: `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`
- [ ] Verify SSL certificates configured
- [ ] Backup existing databases
- [ ] Review resource allocation
- [ ] Enable monitoring (Prometheus/Grafana)
- [ ] Configure alerts
- [ ] Test backup/recovery procedure

### Deployment Steps

```bash
# 1. Validate configuration
docker-compose -f docker-compose.yml -f docker-compose.prod.yml config

# 2. Start services with rolling deployment
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# 3. Monitor logs
docker-compose -f docker-compose.yml -f docker-compose.prod.yml logs -f

# 4. Verify all services healthy
docker-compose -f docker-compose.yml -f docker-compose.prod.yml ps

# 5. Run smoke tests
curl http://localhost:8080/health
curl http://localhost:9000/minio/health/live
curl http://localhost:8081/api/v1/applications
```

---

## Troubleshooting

### Service Won't Start

```bash
# Check logs
docker-compose logs <service_name>

# Increase verbosity
docker-compose logs -f airflow-scheduler 2>&1 | head -100

# Check resource limits
docker stats
```

### Database Connection Issues

```bash
# Test PostgreSQL connection
docker-compose exec postgres psql -U airflow_meta -c "SELECT 1"

# Check connection string
echo $AIRFLOW__DATABASE__SQL_ALCHEMY_CONN

# Reset database (CAUTION: data loss)
docker-compose exec postgres dropdb -U airflow_meta airflow
docker-compose exec postgres createdb -U airflow_meta airflow
```

### Airflow UI Not Responsive

```bash
# Check webserver status
docker-compose logs airflow-web | tail -50

# Restart webserver
docker-compose restart airflow-web

# Check port availability
netstat -an | grep 8080
```

### Out of Disk Space

```bash
# Check disk usage
docker system df

# Clean up old containers
docker container prune

# Clean up unused volumes
docker volume prune

# Clean up logs
docker-compose exec airflow-web rm -rf /opt/airflow/logs/*

# Remove old images
docker image prune -a
```

### Memory Issues

```bash
# Check memory usage
docker stats

# Reduce resource limits in .env
SPARK_EXECUTOR_MEMORY=256m
SPARK_EXECUTOR_CORES=1

# Restart services
docker-compose restart
```

---

## Maintenance

### Regular Tasks

#### Daily
- [ ] Check monitoring dashboards
- [ ] Review failed DAG runs
- [ ] Monitor disk space usage
- [ ] Check for alerts

#### Weekly
- [ ] Review logs for errors
- [ ] Backup critical databases
- [ ] Performance analysis
- [ ] Update dependencies

#### Monthly
- [ ] Full backup verification
- [ ] Disaster recovery test
- [ ] Update base images
- [ ] Security scanning

### Backup Procedures

```bash
# Backup PostgreSQL
docker-compose exec postgres pg_dump -U airflow_meta airflow > backup_airflow_$(date +%Y%m%d).sql

# Backup MinIO data
docker-compose exec minio mc mirror minio/bronze /backups/bronze_$(date +%Y%m%d)

# Backup all volumes
tar -czf backup_all_$(date +%Y%m%d).tar.gz /var/lib/docker/volumes/*
```

### Recovery Procedures

```bash
# Restore PostgreSQL
docker-compose exec postgres psql -U airflow_meta airflow < backup_airflow_20240423.sql

# Restore MinIO
docker-compose exec minio mc mirror /backups/bronze_20240423 minio/bronze
```

---

## Security

### Best Practices

1. **Secrets Management**
   - Never commit .env files
   - Use environment variable substitution
   - Rotate credentials regularly
   - Enable RBAC in Airflow

2. **Network Security**
   - Use isolated bridge networks
   - Enable TLS/SSL for production
   - Configure firewall rules
   - Use VPN for external access

3. **Access Control**
   - Use strong passwords (min 24 chars)
   - Separate user roles (warehouse_user, analytics_user)
   - Enable audit logging
   - Review access regularly

4. **Database Security**
   - Use separate users for each component
   - Apply principle of least privilege
   - Enable connection logging
   - Encrypt connections

### Credential Rotation

```bash
# Generate new passwords
python -c "import secrets; print(secrets.token_urlsafe(24))"

# Update .env files
# Update database users:

docker-compose exec postgres psql -U postgres << EOF
ALTER USER airflow_meta WITH PASSWORD 'new_password_here';
ALTER USER warehouse_user WITH PASSWORD 'new_password_here';
ALTER USER analytics_user WITH PASSWORD 'new_password_here';
EOF

# Restart affected services
docker-compose restart airflow-web airflow-scheduler
```

---

## Monitoring

### Access Monitoring UIs

| Service | URL | Login |
|---------|-----|-------|
| **Airflow** | http://localhost:8080 | admin/password |
| **MinIO** | http://localhost:9001 | minioadmin/password |
| **Spark** | http://localhost:8081 | - |
| **Prometheus** | http://localhost:9090 | - |
| **Grafana** | http://localhost:3000 | admin/password |

### Key Metrics to Monitor

```
Airflow:
- dag_run.success / .failed
- task.duration
- scheduler.heartbeat
- pool.open_slots

PostgreSQL:
- connections
- transaction_duration
- cache_hit_ratio
- checkpoint_write_time

Spark:
- executor_run_time
- task_runtime
- jvm_heap_used
- task_deserialization_time

System:
- CPU utilization
- Memory usage
- Disk I/O
- Network throughput
```

---

## Backup & Recovery

### Backup Strategy

- **Frequency**: Daily at 2 AM UTC
- **Retention**: 30 days
- **Location**: Separate disk/cloud storage
- **Verification**: Weekly restoration test

### Automated Backup

```bash
# Add to crontab
0 2 * * * cd /path/to/data_platform && docker-compose exec -T postgres pg_dump -U airflow_meta airflow > /backups/airflow_$(date +\%Y\%m\%d).sql

# Test restore monthly
0 2 * * 0 /path/to/data_platform/scripts/backup_test.sh
```

### Disaster Recovery

```bash
# 1. Rebuild infrastructure
docker-compose down -v
docker-compose up -d

# 2. Restore database
docker-compose exec postgres psql -U airflow_meta airflow < /backups/latest_backup.sql

# 3. Restore object storage
docker-compose exec minio mc mirror /backups/minio_data minio/

# 4. Verify integrity
./scripts/health_check.sh

# 5. Resume operations
curl -X POST http://localhost:8080/api/v1/dags/pause -H "Content-Type: application/json" -d '{"is_paused": false}'
```

---

## Support & Resources

- **Airflow Docs**: https://airflow.apache.org/docs/
- **Spark Docs**: https://spark.apache.org/docs/
- **MinIO Docs**: https://docs.min.io/
- **Issues**: Check logs in `/opt/airflow/logs/`
- **Contact**: devops@example.com
