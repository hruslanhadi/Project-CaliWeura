# Deployment Guide

## Development Bootstrapping

```bash
cd data_platform
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml config
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml up -d --build
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml exec airflow-web python /opt/airflow/scripts/generate_data.py
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml exec airflow-web airflow dags list
```

Expected shape:

- Airflow uses `LocalExecutor`
- Spark runs as a single `spark-master`
- `ops_connection_smoke_test` is available

## Production Bootstrapping

Update `.env.production` first:

- `POSTGRES_PASSWORD`
- `PG_WAREHOUSE_PASSWORD`
- `PG_ANALYTICS_PASSWORD`
- `MINIO_ROOT_PASSWORD`
- `AIRFLOW_ADMIN_PASSWORD`
- `FERNET_KEY`
- `REDIS_PASSWORD`

Then deploy:

```bash
cd data_platform
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml config
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml up -d --build
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml logs -f airflow-init
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml up -d --scale airflow-worker=2 --scale spark-worker=2
```

## Post-Deploy Verification

```bash
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml ps
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml exec airflow-web airflow config get-value core executor
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml exec airflow-web airflow connections get spark_default
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml exec airflow-web airflow connections get postgres_warehouse
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml exec airflow-web airflow connections get minio_default
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml exec airflow-web airflow dags test ops_connection_smoke_test 2026-04-23
```

## Rollback Basics

```bash
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml down
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml up -d
```

For persistent data rollback, restore PostgreSQL and MinIO from backups before bringing Airflow back up.
