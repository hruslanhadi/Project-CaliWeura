import sys
from datetime import datetime

from pyspark.sql.functions import col, current_timestamp, lit, when

from job_config import build_spark_session, configure_logger, layer_path, load_runtime_config, should_run


logger = configure_logger(__name__)


def transform_orders():
    runtime = load_runtime_config()
    if not should_run("orders", "silver", runtime):
        logger.info("Skipping orders silver job for runtime=%s", runtime)
        return 0

    spark = build_spark_session("OrdersTransformation")
    bronze_df = spark.read.parquet(layer_path("bronze", "orders"))
    silver_df = (
        bronze_df.filter(col("order_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .filter(col("product_id").isNotNull())
        .filter(col("total_amount") > 0)
        .withColumn(
            "net_amount",
            when(
                col("discount_percent").isNotNull(),
                col("total_amount") * (1 - col("discount_percent") / 100),
            ).otherwise(col("total_amount")),
        )
        .withColumn("processed_timestamp", current_timestamp())
        .withColumn("processed_date", lit(datetime.utcnow().date()))
    )
    output_path = layer_path("silver", "orders")
    silver_df.repartition(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_path)
    logger.info("Wrote %s order rows into %s", silver_df.count(), output_path)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(transform_orders())
