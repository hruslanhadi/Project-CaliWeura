"""
Products Transformation Job (Silver Layer)
============================================

Transforms raw product data from bronze to silver layer
- Input: Bronze layer parquet files
- Transformations: Deduplication, validation, enrichment
- Output: Delta Lake in s3://silver/products/

Author: Data Engineering Team
Date: 2024
"""

import logging
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, current_timestamp, lit, when, trim, lower
from pyspark.sql.window import Window
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin_secure_pass')

def transform_products():
    """Transform products data from bronze to silver."""
    
    try:
        logger.info("=" * 80)
        logger.info("🔄 Starting Products Transformation (Bronze → Silver)")
        logger.info("=" * 80)
        
        spark = SparkSession.builder \
            .appName("ProductsTransformation") \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        logger.info("✓ Spark Session initialized")
        
        bronze_path = "s3a://bronze/products"
        bronze_df = spark.read.parquet(bronze_path)
        logger.info(f"✓ Read {bronze_df.count()} records from bronze layer")
        
        # Data Cleaning
        transformed_df = bronze_df \
            .filter(col("product_id").isNotNull()) \
            .filter(col("product_name").isNotNull()) \
            .filter(col("unit_price") > 0) \
            .withColumn("product_name", trim(col("product_name"))) \
            .withColumn("category", trim(col("category"))) \
            .withColumn("brand", trim(col("brand"))) \
            .withColumn("category", lower(col("category"))) \
            .withColumn("brand", lower(col("brand")))
        
        logger.info(f"✓ Cleaned {transformed_df.count()} records")
        
        # Deduplication
        window_spec = Window.partitionBy("product_id").orderBy(col("ingestion_timestamp").desc())
        
        deduplicated_df = transformed_df \
            .withColumn("rn", row_number().over(window_spec)) \
            .filter(col("rn") == 1) \
            .drop("rn")
        
        logger.info(f"✓ Deduplicated to {deduplicated_df.count()} unique products")
        
        # Add enrichment columns
        silver_df = deduplicated_df \
            .withColumn("processed_timestamp", current_timestamp()) \
            .withColumn("processed_date", lit(datetime.now().date())) \
            .withColumn("product_status", lit("active"))
        
        logger.info("✓ Added enrichment columns")
        
        # Write to silver layer
        silver_path = "s3a://silver/products"
        
        silver_df.repartition(1).write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(silver_path)
        
        logger.info(f"✓ Written {silver_df.count()} records to silver layer")
        
        logger.info("=" * 80)
        logger.info(f"📊 Transformation Metrics:")
        logger.info(f"   - Input Records: {bronze_df.count()}")
        logger.info(f"   - Output Records: {silver_df.count()}")
        logger.info(f"   - Records Removed: {bronze_df.count() - silver_df.count()}")
        logger.info("=" * 80)
        logger.info("✅ Products Transformation Completed Successfully")
        
        spark.stop()
        return 0
        
    except Exception as e:
        logger.error(f"❌ Error during transformation: {str(e)}")
        logger.exception(e)
        return 1

if __name__ == "__main__":
    exit_code = transform_products()
    sys.exit(exit_code)
