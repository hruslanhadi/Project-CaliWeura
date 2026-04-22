"""
Customers Ingestion Job (Bronze Layer)
=======================================

Reads customer data from source and writes to MinIO bronze bucket
- Source: Can be CSV, JSON, or simulated data
- Output: Parquet files in s3://bronze/customers/

Author: hruslanhadi
Date: 2026-04-21
"""

import logging
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin_secure_pass')

# ============================================================================
# Schema Definition
# ============================================================================
CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("registration_date", DateType(), True),
])

# ============================================================================
# Main Ingestion Logic
# ============================================================================

def ingest_customers():
    """Main ingestion function for customers data."""
    
    try:
        logger.info("=" * 80)
        logger.info("🚀 Starting Customers Ingestion to Bronze Layer")
        logger.info("=" * 80)
        
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("CustomersBronzeIngestion") \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        logger.info("✓ Spark Session initialized")
        
        # Read customers data
        # In production, this would read from actual data source (API, database, etc.)
        logger.info("Reading customer data from source...")
        
        # Example: Reading from CSV in local data directory
        # In real scenario, adjust path based on your data source
        customers_df = spark.read \
            .schema(CUSTOMERS_SCHEMA) \
            .option("header", "true") \
            .csv("/opt/airflow/data/customers.csv")
        
        logger.info(f"✓ Read {customers_df.count()} customer records")
        
        # Add metadata columns
        from pyspark.sql.functions import current_timestamp, lit, year, month, dayofmonth
        
        customers_df = customers_df.withColumn(
            "ingestion_timestamp", current_timestamp()
        ).withColumn(
            "ingestion_date", lit(datetime.now().date())
        )
        
        logger.info("✓ Added metadata columns")
        
        # Write to MinIO bronze bucket with partitioning
        bronze_path = "s3a://bronze/customers"
        
        customers_df.repartition(4).write \
            .partitionBy("ingestion_date") \
            .mode("overwrite") \
            .parquet(bronze_path)
        
        logger.info(f"✓ Written {customers_df.count()} records to {bronze_path}")
        
        # Log data quality metrics
        logger.info("=" * 80)
        logger.info(f"📊 Ingestion Metrics:")
        logger.info(f"   - Total Records: {customers_df.count()}")
        logger.info(f"   - Columns: {len(customers_df.columns)}")
        logger.info(f"   - Partitions: 4")
        logger.info("=" * 80)
        logger.info("✅ Customers Ingestion Completed Successfully")
        
        spark.stop()
        return 0
        
    except Exception as e:
        logger.error(f"❌ Error during customer ingestion: {str(e)}")
        logger.exception(e)
        return 1

if __name__ == "__main__":
    exit_code = ingest_customers()
    sys.exit(exit_code)
