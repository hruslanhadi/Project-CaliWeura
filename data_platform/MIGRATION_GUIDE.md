# =====================================================
# MIGRATION GUIDE: OLD → NEW CONFIGURATION
# =====================================================
# Step-by-step guide to migrate from hardcoded config to secure, environment-specific setup

## Overview of Changes

### Before (Insecure)
- Hardcoded credentials in docker-compose.yml
- Single configuration for all environments
- No health checks
- No network segmentation
- Weak default passwords

### After (Secure)
- Environment-specific configuration files
- Separate docker-compose files for dev/prod
- Health checks on all services
- Network segmentation
- Secure credential management
- Monitoring and logging infrastructure

---

## Pre-Migration Checklist

- [ ] Backup current running system
- [ ] Backup PostgreSQL databases
- [ ] Document current configuration
- [ ] Notify stakeholders
- [ ] Schedule maintenance window

---

## Migration Steps

### Step 1: Backup Current System

```bash
cd /media/hanafiahrh/DataAll1/Project/Project-CaliWeura/data_platform

# Backup database
docker-compose exec postgres pg_dump -U airflow > backup_airflow_pre_migration.sql

# Backup MinIO data
docker-compose exec minio mc mirror minio/bronze backups/bronze_pre_migration
docker-compose exec minio mc mirror minio/silver backups/silver_pre_migration
docker-compose exec minio mc mirror minio/gold backups/gold_pre_migration

# Backup docker volumes
docker run --rm -v postgres_data:/data -v $(pwd)/backups:/backup alpine tar czf /backup/postgres_data_pre_migration.tar.gz -C / data

# Backup configuration
tar -czf backups/config_pre_migration.tar.gz configs/ dags/ spark_jobs/
```

### Step 2: Stop Current Services

```bash
# Stop old services
docker-compose down

# Verify all stopped
docker-compose ps
```

### Step 3: Review New Configuration Files

```bash
# Files created during setup:
ls -la .env.*
ls -la docker-compose*.yml
ls -la scripts/init_*.sql
cat .env.example  # Review what needs to be configured
```

### Step 4: Configure Environment-Specific Files

#### For Development (16GB Laptop)

```bash
# Copy development template
cp .env.development .env

# Review settings
cat .env

# No changes needed if using defaults, or customize:
# - POSTGRES_PASSWORD
# - MINIO_ROOT_PASSWORD
# - etc.
```

#### For Production

```bash
# Copy production template
cp .env.production .env

# IMPORTANT: Update all passwords and secrets
# Edit .env and change all CHANGE_ME values

# Generate secure passwords
python -c "import secrets; print('POSTGRES_PASSWORD=' + secrets.token_urlsafe(24))"
python -c "import secrets; print('MINIO_ROOT_PASSWORD=' + secrets.token_urlsafe(24))"
python -c "import secrets; print('AIRFLOW_ADMIN_PASSWORD=' + secrets.token_urlsafe(24))"

# Generate Fernet key for Airflow
python -c "from cryptography.fernet import Fernet; print('FERNET_KEY=' + Fernet.generate_key().decode())"

# Add these to .env
nano .env
```

### Step 5: Verify Configuration

```bash
# Validate docker-compose configuration
docker-compose -f docker-compose.yml -f docker-compose.dev.yml config

# Or for production
docker-compose -f docker-compose.yml -f docker-compose.prod.yml config

# Check for validation errors
docker-compose config > /dev/null && echo "✓ Config valid" || echo "✗ Config invalid"
```

### Step 6: Build New Images

```bash
# Build Airflow image with updated Dockerfile
docker-compose -f docker-compose.yml -f docker-compose.dev.yml build

# Tag image
docker image tag caliweura-airflow:latest caliweura-airflow:pre-migration-backup
```

### Step 7: Start New Services

```bash
# For development
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d

# Monitor initialization
docker-compose -f docker-compose.yml -f docker-compose.dev.yml logs -f airflow-init

# Wait for "airflow-init exited with code 0" (2-3 minutes)
```

### Step 8: Restore Data

```bash
# Restore PostgreSQL data
docker-compose exec -T postgres psql -U airflow_meta airflow < backup_airflow_pre_migration.sql

# Verify database
docker-compose exec postgres psql -U airflow_meta -d airflow -c "SELECT COUNT(*) FROM dag;"

# Restore MinIO data (if needed)
# docker-compose exec minio mc mirror backups/bronze_pre_migration minio/bronze
```

### Step 9: Verify Services

```bash
# Check all services running
docker-compose ps

# Expected output:
# caliweura_postgres      running
# caliweura_minio         running
# caliweura_spark_master  running
# caliweura_redis         running
# caliweura_airflow_init  exited (0)
# caliweura_airflow_webserver  running
# caliweura_airflow_scheduler  running
# dp_zookeeper            running
# dp_kafka                running

# Run health checks
./scripts/health_check.sh

# Test Airflow UI
curl http://localhost:8080/health
```

### Step 10: Validation

```bash
# Check database migrations completed
docker-compose exec postgres psql -U airflow_meta -d airflow -c "\dt"

# Verify Airflow can see DAGs
docker-compose logs airflow-scheduler | grep "DAGs parsed"

# List DAGs
docker-compose exec airflow-web airflow dags list

# Check admin user created
docker-compose exec airflow-web airflow users list
```

### Step 11: Test Backup/Restore

```bash
# Create test backup
docker-compose exec -T postgres pg_dump -U airflow_meta airflow > backup_post_migration_test.sql

# Verify backup size
ls -lh backup_post_migration_test.sql

# Test restore to separate database
docker-compose exec postgres psql -U postgres << EOF
CREATE DATABASE airflow_restore_test;
EOF

docker-compose exec -T postgres psql -U airflow_meta -d airflow_restore_test < backup_post_migration_test.sql

# Verify
docker-compose exec postgres psql -U airflow_meta -d airflow_restore_test -c "SELECT COUNT(*) FROM dag;"
```

---

## Post-Migration Steps

### Update Documentation

```bash
# Update existing documentation
cat > MIGRATION_COMPLETE.md << EOF
# Migration Completed

Date: $(date)
From: Old hardcoded configuration
To: Environment-specific secure configuration

## Changes Made
- Environment-specific .env files (.development, .production)
- Separate docker-compose files for dev/prod
- PostgreSQL user separation
- Health checks on all services
- Network segmentation (dp_backend, dp_frontend, dp_data)
- Updated Dockerfile with security improvements
- Structured logging configuration
- Monitoring stack (Prometheus/Grafana)
- Comprehensive runbooks and documentation

## New Files
- .env.example, .env.development, .env.production
- docker-compose.dev.yml, docker-compose.prod.yml
- configs/airflow.cfg, configs/logging_dev.py, configs/logging_prod.py
- scripts/init_warehouse.sql, scripts/init_users.sql
- RUNBOOKS.md, DEPLOYMENT_GUIDE.md, ADR.md

## Verification
- [x] All services running
- [x] Database migrations completed
- [x] Health checks passing
- [x] Airflow UI accessible
- [x] DAGs discoverable
- [x] Backup/restore working

## Next Steps
1. Review new configuration
2. Test disaster recovery
3. Set up monitoring alerts
4. Configure automated backups
5. Enable security hardening
EOF
```

### Security Hardening (Production Only)

```bash
# 1. Change admin password
docker-compose exec airflow-web airflow users delete -u admin
docker-compose exec airflow-web airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@yourdomain.com \
  --password YOUR_SECURE_PASSWORD

# 2. Enable RBAC
# Edit .env:
# AIRFLOW__WEBSERVER__RBAC=True
# AIRFLOW__WEBSERVER__EXPOSE_CONFIG=False

# 3. Configure TLS/SSL
# Place certificates in ./certs/
# Update docker-compose.prod.yml to mount them

# 4. Review and close firewall ports
# Only expose 8080 (Airflow), 9001 (MinIO), 8081 (Spark) if needed

# 5. Enable audit logging
# Review PostgreSQL logs
docker-compose logs postgres | grep "connection"
```

### Monitoring Setup

```bash
# 1. Access Grafana
# http://localhost:3000
# Default: admin/password (change in .env.production)

# 2. Add Prometheus data source
# URL: http://prometheus:9090

# 3. Import dashboards
# - Airflow monitoring dashboard
# - PostgreSQL dashboard
# - Spark dashboard
# - System metrics dashboard
```

### Cleanup

```bash
# After successful migration:

# Remove backup images
docker image rm caliweura-airflow:pre-migration-backup

# Remove old volumes (CAUTION: permanent data loss)
# Only if you're sure everything works:
# docker volume rm caliweura_postgres_data_old

# Remove old docker-compose.yml backup
rm docker-compose.yml.old

# Commit changes to git
git add -A
git commit -m "feat: migrate to secure environment-specific configuration

- Add environment-specific .env files (.development, .production)
- Separate docker-compose files for dev and production
- Implement PostgreSQL user separation with least privilege
- Add health checks to all critical services
- Implement network segmentation
- Upgrade Spark to official Apache image
- Improve Dockerfile with security best practices
- Add structured logging (development and production)
- Add monitoring stack (Prometheus/Grafana)
- Add comprehensive runbooks and documentation
- Update .gitignore to protect credentials"
```

---

## Rollback Plan

If something goes wrong, rollback to the old system:

```bash
# 1. Stop new services
docker-compose -f docker-compose.yml -f docker-compose.dev.yml down

# 2. Restore from backup
docker volume rm postgres_data minio_data airflow_logs
tar -xzf backups/postgres_data_pre_migration.tar.gz -C /var/lib/docker/volumes/

# 3. Restore old configuration
tar -xzf backups/config_pre_migration.tar.gz

# 4. Start with old docker-compose
docker-compose up -d

# 5. Restore database
docker-compose exec postgres psql -U airflow < backup_airflow_pre_migration.sql
```

---

## Verification Checklist

- [ ] All services running (`docker-compose ps`)
- [ ] Health checks passing (`./scripts/health_check.sh`)
- [ ] Airflow UI accessible and responsive
- [ ] DAGs discoverable and schedulable
- [ ] Database contains expected data
- [ ] MinIO buckets and objects present
- [ ] Logs being generated (check `/opt/airflow/logs/`)
- [ ] Monitoring dashboards populated (check Prometheus/Grafana)
- [ ] Backup/restore tested and working
- [ ] Security settings reviewed and validated

---

## Support

For issues during migration:
1. Check logs: `docker-compose logs <service>`
2. Verify configuration: `docker-compose config`
3. Review documentation: See RUNBOOKS.md
4. Check health: `./scripts/health_check.sh`
5. Review this migration guide for step-by-step details

---

## Post-Migration Optimization

After successful migration:

1. **Performance Tuning**
   - Monitor resource usage
   - Adjust memory/CPU limits if needed
   - Optimize DAG configurations

2. **Automation**
   - Set up automated backups
   - Configure monitoring alerts
   - Implement auto-recovery

3. **Documentation**
   - Document customizations
   - Create runbooks for operational tasks
   - Train team on new setup

4. **Security Enhancements**
   - Rotate credentials regularly
   - Enable MFA if possible
   - Implement network policies
   - Set up intrusion detection
