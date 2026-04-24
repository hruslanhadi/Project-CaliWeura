import sys
from datetime import datetime

from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import DateType, StringType, StructField, StructType

from job_config import build_spark_session, configure_logger, data_file, layer_path, load_runtime_config, should_run


logger = configure_logger(__name__)

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("registration_date", DateType(), True),
    ]
)


def ingest_customers():
    runtime = load_runtime_config()
    if not should_run("customers", "bronze", runtime):
        logger.info("Skipping customers bronze job for runtime=%s", runtime)
        return 0

    spark = build_spark_session("CustomersBronzeIngestion")
    df = (
        spark.read.schema(CUSTOMERS_SCHEMA)
        .option("header", "true")
        .csv(data_file("customers.csv"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", lit(datetime.utcnow().date()))
    )
    output_path = layer_path("bronze", "customers")
    df.repartition(1).write.partitionBy("ingestion_date").mode("overwrite").parquet(output_path)
    logger.info("Loaded %s customer rows into %s", df.count(), output_path)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(ingest_customers())
