import os

from pustaka.runtime_config import get_minio_config, get_spark_master, get_spark_packages


def build_spark_conf():
    minio = get_minio_config()
    return {
        # "spark.master": get_spark_master(),
        "spark.master": "spark://spark-master:7077",  # override for testing - ignore
        "spark.executor.memory": os.getenv("SPARK_EXECUTOR_MEMORY", "512m"),
        "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY", "512m"),
        "spark.executor.cores": os.getenv("SPARK_EXECUTOR_CORES", "1"),
        "spark.executor.instances": os.getenv("SPARK_EXECUTOR_INSTANCES", "1"),
        "spark.sql.shuffle.partitions": os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8"),
        "spark.default.parallelism": os.getenv("SPARK_DEFAULT_PARALLELISM", "2"),
        "spark.hadoop.fs.s3a.endpoint": minio["endpoint"],
        "spark.hadoop.fs.s3a.access.key": minio["access_key"],
        "spark.hadoop.fs.s3a.secret.key": minio["secret_key"],
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(minio["endpoint"].startswith("https")).lower(),
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.jars.packages": get_spark_packages(),
    }
