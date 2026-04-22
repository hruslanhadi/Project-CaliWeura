"""
Products Ingestion Job (Bronze Layer)
======================================

Reads product data from source and writes to MinIO bronze bucket
- Source: Can be CSV, JSON, or API
- Output: Parquet files in s3://bronze/products/

Author: hruslanhadi
Date: 2026-04-21
"""

import logging
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin_secure_pass')

# Schema for products
PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("unit_price", DecimalType(10, 2), True),
])

def ingest_products():
    """Main ingestion function for products data."""
    
    try:
        logger.info("=" * 80)
        logger.info("🚀 Starting Products Ingestion to Bronze Layer")
        logger.info("=" * 80)
        
        spark = SparkSession.builder \
            .appName("ProductsBronzeIngestion") \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        logger.info("✓ Spark Session initialized")
        
        products_df = spark.read \
            .schema(PRODUCTS_SCHEMA) \
            .option("header", "true") \
            .csv("/opt/airflow/data/products.csv")
        
        logger.info(f"✓ Read {products_df.count()} product records")
        
        from pyspark.sql.functions import current_timestamp, lit
        
        products_df = products_df.withColumn(
            "ingestion_timestamp", current_timestamp()
        ).withColumn(
            "ingestion_date", lit(datetime.now().date())
        )
        
        logger.info("✓ Added metadata columns")
        
        bronze_path = "s3a://bronze/products"
        
        products_df.repartition(2).write \
            .partitionBy("ingestion_date") \
            .mode("overwrite") \
            .parquet(bronze_path)
        
        logger.info(f"✓ Written {products_df.count()} records to {bronze_path}")
        
        logger.info("=" * 80)
        logger.info(f"📊 Ingestion Metrics:")
        logger.info(f"   - Total Records: {products_df.count()}")
        logger.info(f"   - Columns: {len(products_df.columns)}")
        logger.info("=" * 80)
        logger.info("✅ Products Ingestion Completed Successfully")
        
        spark.stop()
        return 0
        
    except Exception as e:
        logger.error(f"❌ Error during products ingestion: {str(e)}")
        logger.exception(e)
        return 1

if __name__ == "__main__":
    exit_code = ingest_products()
    sys.exit(exit_code)
