# dags/data_platform/spark_config.py

from pustaka.config import MINIO_CONFIG, COMMON_SPARK_CONF

def build_spark_conf():
    return {
        **COMMON_SPARK_CONF,

        # Spark master
        "spark.master": "spark://spark_master:7077",

        # MinIO (S3A)
        "spark.hadoop.fs.s3a.endpoint": MINIO_CONFIG["endpoint"],
        "spark.hadoop.fs.s3a.access.key": MINIO_CONFIG["access_key"],
        "spark.hadoop.fs.s3a.secret.key": MINIO_CONFIG["secret_key"],
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",

        # Delta Lake
        "spark.jars.packages": "io.delta:delta-core_2.12:2.4.0",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    }