import sys
from datetime import datetime

from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from job_config import build_spark_session, configure_logger, data_file, layer_path, load_runtime_config, should_run


logger = configure_logger(__name__)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("unit_price", DecimalType(10, 2), True),
    ]
)


def ingest_products():
    runtime = load_runtime_config()
    if not should_run("products", "bronze", runtime):
        logger.info("Skipping products bronze job for runtime=%s", runtime)
        return 0

    spark = build_spark_session("ProductsBronzeIngestion")
    df = (
        spark.read.schema(PRODUCTS_SCHEMA)
        .option("header", "true")
        .csv(data_file("products.csv"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", lit(datetime.utcnow().date()))
    )
    output_path = layer_path("bronze", "products")
    df.repartition(1).write.partitionBy("ingestion_date").mode("overwrite").parquet(output_path)
    logger.info("Loaded %s product rows into %s", df.count(), output_path)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(ingest_products())
