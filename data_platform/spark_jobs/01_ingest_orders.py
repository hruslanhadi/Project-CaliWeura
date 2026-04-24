import sys
from datetime import datetime

from pyspark.sql.functions import current_timestamp, lit, month, year
from pyspark.sql.types import DateType, DecimalType, IntegerType, StringType, StructField, StructType

from job_config import build_spark_session, configure_logger, data_file, layer_path, load_runtime_config, should_run


logger = configure_logger(__name__)

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("order_date", DateType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DecimalType(10, 2), True),
        StructField("total_amount", DecimalType(12, 2), True),
        StructField("discount_percent", DecimalType(5, 2), True),
    ]
)


def ingest_orders():
    runtime = load_runtime_config()
    if not should_run("orders", "bronze", runtime):
        logger.info("Skipping orders bronze job for runtime=%s", runtime)
        return 0

    spark = build_spark_session("OrdersBronzeIngestion")
    df = (
        spark.read.schema(ORDERS_SCHEMA)
        .option("header", "true")
        .csv(data_file("orders.csv"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", lit(datetime.utcnow().date()))
        .withColumn("year", year("order_date"))
        .withColumn("month", month("order_date"))
    )
    output_path = layer_path("bronze", "orders")
    df.repartition(1).write.partitionBy("year", "month").mode("overwrite").parquet(output_path)
    logger.info("Loaded %s order rows into %s", df.count(), output_path)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(ingest_orders())
