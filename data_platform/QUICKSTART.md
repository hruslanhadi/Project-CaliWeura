# Quick Start

This guide is for local development on a 16 GB laptop.

## Architecture

| Environment | Airflow | Spark | Memory profile | Best for | Scaling |
| --- | --- | --- | --- | --- | --- |
| Development | `LocalExecutor` | single `spark-master` node | conservative | local coding and DAG debugging | none |
| Production | `CeleryExecutor` | standalone `spark-master` + `spark-worker` | 32 GB+ host | parallel task execution | `--scale airflow-worker=N --scale spark-worker=N` |

## Start The Dev Stack

```bash
cd data_platform
chmod +x scripts/*.sh
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml up -d --build
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml exec -T airflow-web python /opt/airflow/scripts/generate_data.py
./scripts/health_check.sh
```

Open:

- Airflow: `http://localhost:8080`
- MinIO: `http://localhost:9001`
- Spark UI: `http://localhost:8081`

Default dev credentials:

- Airflow: `admin / admin123`
- MinIO: `minioadmin / minioadmin_dev_pass`
- Warehouse DB: `warehouse_user / warehouse_dev_pass`

## Trigger The Pipeline

1. Open Airflow and enable `data_platform_medallion_pipeline`.
2. Click `Trigger DAG w/ config`.
3. Use one of these JSON payloads.

Default incremental run:

```json
{}
```

Selected datasets to bronze only:

```json
{
  "datasets": ["customers", "orders"],
  "target_layer": "bronze"
}
```

Manual repair window:

```json
{
  "run_mode": "incremental",
  "start_date": "2026-04-01",
  "end_date": "2026-04-07",
  "datasets": ["all"],
  "skip_quality_checks": false,
  "target_layer": "all"
}
```

## DAG Parameters

The pipeline exposes these manual-trigger parameters:

- `run_mode`: `incremental` or `full_refresh`
- `start_date`: optional ISO date
- `end_date`: optional ISO date
- `datasets`: `"all"` or a list from `customers`, `products`, `orders`
- `skip_quality_checks`: `true` or `false`
- `target_layer`: `bronze`, `silver`, `gold`, or `all`

Behavior:

1. Airflow opens the trigger dialog and accepts JSON.
2. `validate_runtime_params` checks values and fills default dates from the run window.
3. Bronze and silver tasks skip automatically when the chosen dataset or layer does not apply.
4. Spark tasks receive the validated runtime config as application arguments and env vars.
5. Gold warehouse loading and SQL aggregations run only when `target_layer` includes `gold` or `all`.
6. Invalid date windows or gold runs with partial datasets fail before Spark submission.

## Smoke Tests

```bash
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml exec airflow-web pytest tests/test_dag_integrity.py tests/test_integration.py
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml exec airflow-web airflow dags test ops_connection_smoke_test 2026-04-23
```

## Useful Commands

```bash
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml logs -f airflow-scheduler
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml down
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml exec airflow-web airflow dags list
docker compose --env-file .env.development -f docker-compose.yml -f docker-compose.dev.yml exec postgres psql -U warehouse_user -d data_warehouse
```
