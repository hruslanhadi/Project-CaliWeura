"""
Orders Transformation Job (Silver Layer)
==========================================

Transforms raw order data from bronze to silver layer
- Input: Bronze layer parquet files
- Transformations: Validation, enrichment, join with masters
- Output: Delta Lake in s3://silver/orders/

Author: Data Engineering Team
Date: 2024
"""

import logging
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin_secure_pass')

def transform_orders():
    """Transform orders data from bronze to silver."""
    
    try:
        logger.info("=" * 80)
        logger.info("🔄 Starting Orders Transformation (Bronze → Silver)")
        logger.info("=" * 80)
        
        spark = SparkSession.builder \
            .appName("OrdersTransformation") \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        logger.info("✓ Spark Session initialized")
        
        # Read bronze data
        bronze_path = "s3a://bronze/orders"
        bronze_df = spark.read.parquet(bronze_path)
        logger.info(f"✓ Read {bronze_df.count()} records from bronze layer")
        
        # Data Validation & Cleaning
        transformed_df = bronze_df \
            .filter(col("order_id").isNotNull()) \
            .filter(col("customer_id").isNotNull()) \
            .filter(col("product_id").isNotNull()) \
            .filter(col("total_amount") > 0) \
            .withColumn("net_amount", 
                when(col("discount_percent").isNotNull(),
                    col("total_amount") * (1 - col("discount_percent") / 100)
                ).otherwise(col("total_amount"))
            )
        
        logger.info(f"✓ Validated {transformed_df.count()} orders")
        
        # Add business logic columns
        silver_df = transformed_df \
            .withColumn("order_month", lit(datetime.now().month)) \
            .withColumn("order_year", lit(datetime.now().year)) \
            .withColumn("processed_timestamp", current_timestamp()) \
            .withColumn("processed_date", lit(datetime.now().date()))
        
        logger.info("✓ Added business logic columns")
        
        # Write to silver layer
        silver_path = "s3a://silver/orders"
        
        silver_df.repartition(4).write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(silver_path)
        
        logger.info(f"✓ Written {silver_df.count()} records to silver layer")
        
        # Aggregation stats
        total_revenue = silver_df.agg({"total_amount": "sum"}).collect()[0][0]
        avg_order_value = silver_df.agg({"total_amount": "avg"}).collect()[0][0]
        
        logger.info("=" * 80)
        logger.info(f"📊 Transformation Metrics:")
        logger.info(f"   - Input Records: {bronze_df.count()}")
        logger.info(f"   - Output Records: {silver_df.count()}")
        logger.info(f"   - Records Removed: {bronze_df.count() - silver_df.count()}")
        logger.info(f"   - Total Revenue: ${total_revenue:.2f}")
        logger.info(f"   - Average Order Value: ${avg_order_value:.2f}")
        logger.info("=" * 80)
        logger.info("✅ Orders Transformation Completed Successfully")
        
        spark.stop()
        return 0
        
    except Exception as e:
        logger.error(f"❌ Error during transformation: {str(e)}")
        logger.exception(e)
        return 1

if __name__ == "__main__":
    exit_code = transform_orders()
    sys.exit(exit_code)
