import sys
from datetime import datetime

from pyspark.sql.functions import col, current_timestamp, lit, lower, row_number, trim
from pyspark.sql.window import Window

from job_config import build_spark_session, configure_logger, layer_path, load_runtime_config, should_run


logger = configure_logger(__name__)


def transform_products():
    runtime = load_runtime_config()
    if not should_run("products", "silver", runtime):
        logger.info("Skipping products silver job for runtime=%s", runtime)
        return 0

    spark = build_spark_session("ProductsTransformation")
    bronze_df = spark.read.parquet(layer_path("bronze", "products"))
    transformed_df = (
        bronze_df.filter(col("product_id").isNotNull())
        .filter(col("product_name").isNotNull())
        .filter(col("unit_price") > 0)
        .withColumn("product_name", trim(col("product_name")))
        .withColumn("category", lower(trim(col("category"))))
        .withColumn("subcategory", trim(col("subcategory")))
        .withColumn("brand", lower(trim(col("brand"))))
    )
    window_spec = Window.partitionBy("product_id").orderBy(col("ingestion_timestamp").desc())
    silver_df = (
        transformed_df.withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
        .withColumn("processed_timestamp", current_timestamp())
        .withColumn("processed_date", lit(datetime.utcnow().date()))
        .withColumn("product_status", lit("active"))
    )
    output_path = layer_path("silver", "products")
    silver_df.repartition(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_path)
    logger.info("Wrote %s product rows into %s", silver_df.count(), output_path)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(transform_products())
