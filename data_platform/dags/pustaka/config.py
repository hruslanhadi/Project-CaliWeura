# dags/data_platform/config.py

SPARK_CONN_ID = "spark_default"

SPARK_MASTER = "spark://spark_master:7077"

MINIO_CONFIG = {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin_secure_pass",
}

POSTGRES_WAREHOUSE_CONN = "postgres_warehouse"

COMMON_SPARK_CONF = {
    "spark.executor.memory": "1g",
    "spark.driver.memory": "1g",
    "spark.executor.cores": "2",
}