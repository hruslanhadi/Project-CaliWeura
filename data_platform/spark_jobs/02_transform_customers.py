import sys
from datetime import datetime

from pyspark.sql.functions import col, current_timestamp, lit, lower, row_number, trim, when
from pyspark.sql.window import Window

from job_config import build_spark_session, configure_logger, layer_path, load_runtime_config, should_run


logger = configure_logger(__name__)


def transform_customers():
    runtime = load_runtime_config()
    if not should_run("customers", "silver", runtime):
        logger.info("Skipping customers silver job for runtime=%s", runtime)
        return 0

    spark = build_spark_session("CustomersTransformation")
    bronze_df = spark.read.parquet(layer_path("bronze", "customers"))
    transformed_df = (
        bronze_df.filter(col("customer_id").isNotNull())
        .filter(col("email").isNotNull())
        .withColumn("first_name", trim(col("first_name")))
        .withColumn("last_name", trim(col("last_name")))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("phone", when(col("phone") != "", col("phone")).otherwise(None))
    )
    window_spec = Window.partitionBy("customer_id").orderBy(col("ingestion_timestamp").desc())
    silver_df = (
        transformed_df.withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
        .withColumn("processed_timestamp", current_timestamp())
        .withColumn("processed_date", lit(datetime.utcnow().date()))
        .withColumn("data_quality_score", lit(1.0))
    )
    output_path = layer_path("silver", "customers")
    silver_df.repartition(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_path)
    logger.info("Wrote %s customer rows into %s", silver_df.count(), output_path)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(transform_customers())
