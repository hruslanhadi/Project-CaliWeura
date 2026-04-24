import argparse
import json
import logging
import os
from urllib.parse import urlparse

from pyspark.sql import SparkSession


def configure_logger(name):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    return logging.getLogger(name)


def load_runtime_config():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--runtime-config", default=os.getenv("PIPELINE_RUNTIME_CONFIG", "{}"))
    args, _ = parser.parse_known_args()
    raw = args.runtime_config or "{}"
    config = json.loads(raw) if isinstance(raw, str) else dict(raw)
    datasets = config.get("datasets", ["all"])
    if isinstance(datasets, str):
        datasets = [item.strip() for item in datasets.split(",") if item.strip()]
    config["datasets"] = datasets or ["all"]
    config.setdefault("target_layer", "all")
    config.setdefault("run_mode", "incremental")
    config.setdefault("skip_quality_checks", False)
    return config


def should_run(dataset, layer, runtime_config):
    order = {"bronze": 1, "silver": 2, "gold": 3, "all": 3}
    selected_datasets = runtime_config.get("datasets", ["all"])
    dataset_ok = "all" in selected_datasets or dataset in selected_datasets
    layer_ok = order[layer] <= order[runtime_config.get("target_layer", "all")]
    return dataset_ok and layer_ok


def normalize_endpoint(endpoint):
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        return endpoint
    return f"http://{endpoint}"


def get_storage_config():
    return {
        "endpoint": normalize_endpoint(os.getenv("MINIO_ENDPOINT", "minio:9000")),
        "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin_dev_pass"),
        "bronze_bucket": os.getenv("S3_BUCKET_BRONZE", "bronze"),
        "silver_bucket": os.getenv("S3_BUCKET_SILVER", "silver"),
        "gold_bucket": os.getenv("S3_BUCKET_GOLD", "gold"),
    }


def get_warehouse_config():
    return {
        "host": os.getenv("PG_HOST", "postgres"),
        "port": os.getenv("PG_PORT", "5432"),
        "database": os.getenv("PG_DB", "data_warehouse"),
        "user": os.getenv("PG_USER", "warehouse_user"),
        "password": os.getenv("PG_PASSWORD", "warehouse_dev_pass"),
        "jdbc_url": os.getenv("PG_JDBC_URL"),
    }


def build_spark_session(app_name):
    storage = get_storage_config()
    endpoint = storage["endpoint"]
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", storage["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", storage["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https")).lower())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return builder.getOrCreate()


def data_file(name):
    return os.path.join(os.getenv("DATA_DIR", "/opt/airflow/data"), name)


def layer_path(layer, dataset):
    storage = get_storage_config()
    bucket = storage[f"{layer}_bucket"]
    return f"s3a://{bucket}/{dataset}"
