# Runbooks

## Environment Overview

| Environment | Compose command | Executor | Spark topology | Notes |
| --- | --- | --- | --- | --- |
| Development | `docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d` | `LocalExecutor` | one `spark-master` | tuned for a 16 GB laptop |
| Production | `docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d` | `CeleryExecutor` | `spark-master` plus scalable `spark-worker` | tuned for 32 GB+ single-host deployment |

## Development Operations

Start:

```bash
cd data_platform
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml up -d --build
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml exec airflow-web python /opt/airflow/scripts/generate_data.py
```

Validate:

```bash
./scripts/health_check.sh
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml exec airflow-web pytest tests/test_dag_integrity.py tests/test_integration.py
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml exec airflow-web airflow dags test ops_connection_smoke_test 2026-04-23
```

Stop:

```bash
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml down
```

Optional streaming profile:

```bash
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml --profile streaming up -d
```

## Production Operations

Start:

```bash
cd data_platform
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml config
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml up -d --build
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml logs -f airflow-init
```

Scale:

```bash
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml up -d --scale airflow-worker=2 --scale spark-worker=2
```

Verify:

```bash
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml ps
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml exec airflow-web airflow config get-value core executor
docker compose --env-file .env.production -f docker-compose.yml -f docker-compose.prod.yml exec airflow-web airflow dags list | grep ops_connection_smoke_test
curl http://localhost:8080/health
curl http://localhost:8081
curl http://localhost:9001
```

## Connection Tests

Airflow-driven smoke test:

```bash
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml exec airflow-web airflow dags test ops_connection_smoke_test 2026-04-23
```

What it checks:

- Airflow Python runtime
- PostgreSQL warehouse connection via `postgres_warehouse`
- MinIO via `minio_default`
- Spark submit via `spark_default`

## DAG Parameters

Manual trigger steps:

1. Open `data_platform_medallion_pipeline`.
2. Click `Trigger DAG w/ config`.
3. Provide JSON.
4. Watch `validate_runtime_params` first.
5. Inspect skipped tasks to confirm dataset/layer filtering.

Examples:

Full historical reload:

```json
{
  "run_mode": "full_refresh",
  "datasets": ["all"],
  "target_layer": "all",
  "skip_quality_checks": false
}
```

One-day incremental repair:

```json
{
  "run_mode": "incremental",
  "start_date": "2026-04-22",
  "end_date": "2026-04-22",
  "datasets": ["all"],
  "target_layer": "all"
}
```

Bronze-only backfill for selected datasets:

```json
{
  "run_mode": "incremental",
  "start_date": "2026-04-01",
  "end_date": "2026-04-07",
  "datasets": ["customers", "orders"],
  "target_layer": "bronze",
  "skip_quality_checks": true
}
```

## Troubleshooting

Airflow not healthy:

```bash
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml logs -f airflow-web airflow-scheduler
```

Warehouse connection issues:

```bash
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml exec postgres psql -U postgres -d postgres -c "\du"
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml exec airflow-web airflow connections get postgres_warehouse
```

MinIO buckets missing:

```bash
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml exec minio mc alias set local http://minio:9000 minioadmin minioadmin_dev_pass
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml exec minio mc ls local
```

Spark submit failures:

```bash
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml logs -f spark-master
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml exec airflow-web spark-submit /opt/airflow/spark_jobs/test_spark.py
```
