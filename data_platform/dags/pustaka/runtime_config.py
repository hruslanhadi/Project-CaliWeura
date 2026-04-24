import os
from functools import lru_cache
from urllib.parse import quote_plus

from airflow.hooks.base import BaseHook
from airflow.models import Variable

from pustaka.config import (
    DEFAULT_DATA_DIR,
    DEFAULT_MINIO_HOST,
    DEFAULT_MINIO_PORT,
    DEFAULT_MINIO_SCHEME,
    DEFAULT_SPARK_MASTER,
    MINIO_CONN_ID,
    POSTGRES_WAREHOUSE_CONN,
    SPARK_CONN_ID,
)


def _safe_connection(conn_id):
    try:
        return BaseHook.get_connection(conn_id)
    except Exception:
        return None


def _env(name, default=None):
    return os.getenv(name, default)


def get_platform_env():
    return Variable.get("platform_env", default_var=_env("ENVIRONMENT", "development"))


@lru_cache(maxsize=1)
def get_spark_master():
    conn = _safe_connection(SPARK_CONN_ID)
    if conn and conn.host:
        scheme = conn.conn_type or "spark"
        port = conn.port or 7077
        return f"{scheme}://{conn.host}:{port}"
    return _env("SPARK_MASTER", DEFAULT_SPARK_MASTER)


@lru_cache(maxsize=1)
def get_minio_config():
    conn = _safe_connection(MINIO_CONN_ID)
    if conn and conn.host:
        scheme = conn.schema or conn.conn_type or DEFAULT_MINIO_SCHEME
        port = conn.port or DEFAULT_MINIO_PORT
        return {
            "endpoint": f"{scheme}://{conn.host}:{port}",
            "access_key": conn.login or _env("MINIO_ROOT_USER", "minioadmin"),
            "secret_key": conn.password or _env("MINIO_ROOT_PASSWORD", "minioadmin_dev_pass"),
            "bronze_bucket": _env("S3_BUCKET_BRONZE", "bronze"),
            "silver_bucket": _env("S3_BUCKET_SILVER", "silver"),
            "gold_bucket": _env("S3_BUCKET_GOLD", "gold"),
        }
    return {
        "endpoint": f"{_env('MINIO_SCHEME', DEFAULT_MINIO_SCHEME)}://{_env('MINIO_ENDPOINT', f'{DEFAULT_MINIO_HOST}:{DEFAULT_MINIO_PORT}')}",
        "access_key": _env("MINIO_ROOT_USER", "minioadmin"),
        "secret_key": _env("MINIO_ROOT_PASSWORD", "minioadmin_dev_pass"),
        "bronze_bucket": _env("S3_BUCKET_BRONZE", "bronze"),
        "silver_bucket": _env("S3_BUCKET_SILVER", "silver"),
        "gold_bucket": _env("S3_BUCKET_GOLD", "gold"),
    }


@lru_cache(maxsize=1)
def get_warehouse_config():
    conn = _safe_connection(POSTGRES_WAREHOUSE_CONN)
    if conn and conn.host:
        schema = conn.schema or "data_warehouse"
        return {
            "host": conn.host,
            "port": conn.port or 5432,
            "database": schema,
            "user": conn.login or "warehouse_user",
            "password": conn.password or "",
            "jdbc_url": f"jdbc:postgresql://{conn.host}:{conn.port or 5432}/{schema}",
        }

    host = _env("PG_WAREHOUSE_HOST", "postgres")
    port = int(_env("PG_WAREHOUSE_PORT", "5432"))
    database = _env("PG_WAREHOUSE_DB", "data_warehouse")
    user = _env("PG_WAREHOUSE_USER", "warehouse_user")
    password = _env("PG_WAREHOUSE_PASSWORD", "warehouse_dev_pass")
    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "jdbc_url": f"jdbc:postgresql://{host}:{port}/{database}",
    }


def get_default_task_env(include_warehouse=False):
    minio = get_minio_config()
    env = {
        "PLATFORM_ENV": get_platform_env(),
        "DATA_DIR": _env("DATA_DIR", DEFAULT_DATA_DIR),
        "MINIO_ENDPOINT": minio["endpoint"],
        "MINIO_ACCESS_KEY": minio["access_key"],
        "MINIO_SECRET_KEY": minio["secret_key"],
        "S3_BUCKET_BRONZE": minio["bronze_bucket"],
        "S3_BUCKET_SILVER": minio["silver_bucket"],
        "S3_BUCKET_GOLD": minio["gold_bucket"],
        "SPARK_MASTER": get_spark_master(),
        "SPARK_DRIVER_MEMORY": _env("SPARK_DRIVER_MEMORY", "512m"),
        "SPARK_EXECUTOR_MEMORY": _env("SPARK_EXECUTOR_MEMORY", "512m"),
        "SPARK_EXECUTOR_CORES": _env("SPARK_EXECUTOR_CORES", "1"),
        "SPARK_EXECUTOR_INSTANCES": _env("SPARK_EXECUTOR_INSTANCES", "1"),
        "SPARK_SQL_SHUFFLE_PARTITIONS": _env("SPARK_SQL_SHUFFLE_PARTITIONS", "8"),
        "SPARK_DEFAULT_PARALLELISM": _env("SPARK_DEFAULT_PARALLELISM", "2"),
        "PYTHONPATH": "/opt/airflow:/opt/airflow/dags:/opt/airflow/spark_jobs",
    }

    if include_warehouse:
        warehouse = get_warehouse_config()
        env.update(
            {
                "PG_HOST": warehouse["host"],
                "PG_PORT": str(warehouse["port"]),
                "PG_DB": warehouse["database"],
                "PG_USER": warehouse["user"],
                "PG_PASSWORD": warehouse["password"],
                "PG_JDBC_URL": warehouse["jdbc_url"],
            }
        )

    return env


def get_spark_packages():
    return ",".join(
        [
            "io.delta:delta-core_2.12:2.4.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            "org.postgresql:postgresql:42.7.3",
        ]
    )


def get_postgres_sqlalchemy_uri():
    warehouse = get_warehouse_config()
    password = quote_plus(warehouse["password"])
    return (
        f"postgresql+psycopg2://{warehouse['user']}:{password}"
        f"@{warehouse['host']}:{warehouse['port']}/{warehouse['database']}"
    )
