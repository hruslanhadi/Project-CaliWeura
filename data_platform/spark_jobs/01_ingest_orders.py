"""
Orders Ingestion Job (Bronze Layer)
====================================

Reads order/sales transaction data from source and writes to MinIO bronze bucket
- Source: Can be CSV, JSON, database, or Kafka
- Output: Parquet files in s3://bronze/orders/

Author: hruslanhadi
Date: 2026-04-21
"""

import logging
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, IntegerType
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin_secure_pass')

# Schema for orders/sales
ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("order_date", DateType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(10, 2), True),
    StructField("total_amount", DecimalType(12, 2), True),
    StructField("discount_percent", DecimalType(5, 2), True),
])

def ingest_orders():
    """Main ingestion function for orders data."""
    
    try:
        logger.info("=" * 80)
        logger.info("🚀 Starting Orders Ingestion to Bronze Layer")
        logger.info("=" * 80)
        
        spark = SparkSession.builder \
            .appName("OrdersBronzeIngestion") \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        logger.info("✓ Spark Session initialized")
        
        orders_df = spark.read \
            .schema(ORDERS_SCHEMA) \
            .option("header", "true") \
            .csv("/opt/airflow/data/orders.csv")
        
        logger.info(f"✓ Read {orders_df.count()} order records")
        
        from pyspark.sql.functions import current_timestamp, lit, year, month
        
        # Add metadata and partitioning columns
        orders_df = orders_df.withColumn(
            "ingestion_timestamp", current_timestamp()
        ).withColumn(
            "ingestion_date", lit(datetime.now().date())
        ).withColumn(
            "year", year("order_date")
        ).withColumn(
            "month", month("order_date")
        )
        
        logger.info("✓ Added metadata columns")
        
        bronze_path = "s3a://bronze/orders"
        
        # Write with year/month partitioning for efficiency
        orders_df.repartition(4).write \
            .partitionBy("year", "month") \
            .mode("overwrite") \
            .parquet(bronze_path)
        
        logger.info(f"✓ Written {orders_df.count()} records to {bronze_path}")
        
        logger.info("=" * 80)
        logger.info(f"📊 Ingestion Metrics:")
        logger.info(f"   - Total Records: {orders_df.count()}")
        logger.info(f"   - Date Range: {orders_df.agg({'order_date': 'min'}).collect()} - {orders_df.agg({'order_date': 'max'}).collect()}")
        logger.info(f"   - Total Revenue: ${orders_df.agg({'total_amount': 'sum'}).collect()}")
        logger.info("=" * 80)
        logger.info("✅ Orders Ingestion Completed Successfully")
        
        spark.stop()
        return 0
        
    except Exception as e:
        logger.error(f"❌ Error during orders ingestion: {str(e)}")
        logger.exception(e)
        return 1

if __name__ == "__main__":
    exit_code = ingest_orders()
    sys.exit(exit_code)
