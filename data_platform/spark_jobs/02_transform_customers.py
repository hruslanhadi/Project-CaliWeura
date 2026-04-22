"""
Customers Transformation Job (Silver Layer)
=============================================

Transforms raw customer data from bronze to silver layer
- Input: Bronze layer parquet files
- Transformations: Deduplication, validation, enrichment
- Output: Delta Lake in s3://silver/customers/

Author: Data Engineering Team
Date: 2024
"""

import logging
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, current_timestamp, lit, when, trim, upper
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

def transform_customers():
    """Transform customers data from bronze to silver."""
    
    try:
        logger.info("=" * 80)
        logger.info("🔄 Starting Customers Transformation (Bronze → Silver)")
        logger.info("=" * 80)
        
        spark = SparkSession.builder \
            .appName("CustomersTransformation") \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        logger.info("✓ Spark Session initialized")
        
        # Read from bronze layer
        bronze_path = "s3a://bronze/customers"
        bronze_df = spark.read.parquet(bronze_path)
        logger.info(f"✓ Read {bronze_df.count()} records from bronze layer")
        
        # Data Quality & Cleaning Transformations
        transformed_df = bronze_df \
            .filter(col("customer_id").isNotNull()) \
            .filter(col("email").isNotNull()) \
            .withColumn("first_name", trim(col("first_name"))) \
            .withColumn("last_name", trim(col("last_name"))) \
            .withColumn("email", lower(trim(col("email")))) \
            .withColumn("phone", when(col("phone") != "", col("phone")).otherwise(None))
        
        logger.info(f"✓ Cleaned {transformed_df.count()} records")
        
        # Deduplication - Keep latest record per customer
        window_spec = Window.partitionBy("customer_id").orderBy(col("ingestion_timestamp").desc())
        
        deduplicated_df = transformed_df \
            .withColumn("rn", row_number().over(window_spec)) \
            .filter(col("rn") == 1) \
            .drop("rn")
        
        logger.info(f"✓ Deduplicated to {deduplicated_df.count()} unique customers")
        
        # Add technical columns
        silver_df = deduplicated_df \
            .withColumn("processed_timestamp", current_timestamp()) \
            .withColumn("processed_date", lit(datetime.now().date())) \
            .withColumn("data_quality_score", lit(1.0))
        
        logger.info("✓ Added processing metadata")
        
        # Write to silver layer (Delta Lake format)
        silver_path = "s3a://silver/customers"
        
        silver_df.repartition(2).write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(silver_path)
        
        logger.info(f"✓ Written {silver_df.count()} records to silver layer")
        
        # Data Quality Checks
        null_check = silver_df.filter(col("customer_id").isNull()).count()
        duplicate_check = silver_df.groupBy("customer_id").count().filter(col("count") > 1).count()
        
        logger.info("=" * 80)
        logger.info(f"📊 Transformation Metrics:")
        logger.info(f"   - Input Records: {bronze_df.count()}")
        logger.info(f"   - Output Records: {silver_df.count()}")
        logger.info(f"   - Records Removed: {bronze_df.count() - silver_df.count()}")
        logger.info(f"   - Null Customer IDs: {null_check}")
        logger.info(f"   - Duplicate Customers: {duplicate_check}")
        logger.info("=" * 80)
        logger.info("✅ Customers Transformation Completed Successfully")
        
        spark.stop()
        return 0
        
    except Exception as e:
        logger.error(f"❌ Error during transformation: {str(e)}")
        logger.exception(e)
        return 1

if __name__ == "__main__":
    exit_code = transform_customers()
    sys.exit(exit_code)
