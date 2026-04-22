"""
Gold Layer Aggregation & Warehouse Loading
============================================

Aggregates silver layer data and loads into PostgreSQL warehouse
- Input: Silver layer Delta Lake files
- Transformations: Join dimensions, aggregate metrics
- Output: PostgreSQL tables (star schema)

This job:
1. Reads transformed data from silver layer
2. Performs star schema joins
3. Loads fact and dimension tables to PostgreSQL
4. Computes aggregation tables

Author: Data Engineering Team
Date: 2024
"""

import logging
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, current_timestamp, lit, when, max as spark_max
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

PG_HOST = os.getenv('PG_HOST', 'postgres')
PG_USER = os.getenv('PG_USER', 'warehouse_user')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'warehouse_pass')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DB = os.getenv('PG_DB', 'data_warehouse')

def aggregate_to_warehouse():
    """Aggregate silver layer data and load to warehouse."""
    
    try:
        logger.info("=" * 80)
        logger.info("⭐ Starting Gold Layer Aggregation & Warehouse Loading")
        logger.info("=" * 80)
        
        spark = SparkSession.builder \
            .appName("GoldLayerAggregation") \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        logger.info("✓ Spark Session initialized")
        
        # PostgreSQL connection properties
        pg_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
        pg_props = {
            "user": PG_USER,
            "password": PG_PASSWORD,
            "driver": "org.postgresql.Driver"
        }
        
        # Read silver layer data
        logger.info("Reading silver layer data...")
        
        customers_df = spark.read.format("delta").load("s3a://silver/customers")
        products_df = spark.read.format("delta").load("s3a://silver/products")
        orders_df = spark.read.format("delta").load("s3a://silver/orders")
        
        logger.info(f"✓ Customers: {customers_df.count()}")
        logger.info(f"✓ Products: {products_df.count()}")
        logger.info(f"✓ Orders: {orders_df.count()}")
        
        # =====================================================================
        # Load Dimension Tables
        # =====================================================================
        
        logger.info("Loading dimension tables to PostgreSQL...")
        
        # Prepare customers dimension
        dim_customers = customers_df.select(
            col("customer_id").alias("external_customer_id"),
            col("first_name"),
            col("last_name"),
            col("email"),
            col("country"),
            col("city"),
            col("phone"),
            col("registration_date"),
            current_timestamp().alias("created_at")
        ).distinct()
        
        dim_customers.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", pg_url) \
            .option("dbtable", "public.dim_customers") \
            .options(**pg_props) \
            .save()
        
        logger.info(f"✓ Loaded {dim_customers.count()} customer dimensions")
        
        # Prepare products dimension
        dim_products = products_df.select(
            col("product_id").alias("external_product_id"),
            col("product_name"),
            col("category"),
            col("subcategory"),
            col("brand"),
            col("unit_price"),
            current_timestamp().alias("created_at")
        ).distinct()
        
        dim_products.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", pg_url) \
            .option("dbtable", "public.dim_products") \
            .options(**pg_props) \
            .save()
        
        logger.info(f"✓ Loaded {dim_products.count()} product dimensions")
        
        # =====================================================================
        # Load Fact Tables
        # =====================================================================
        
        logger.info("Loading fact tables to PostgreSQL...")
        
        # Prepare sales fact table
        fact_sales = orders_df.select(
            col("order_id"),
            col("customer_id"),
            col("product_id"),
            col("order_date"),
            col("quantity"),
            col("unit_price"),
            col("total_amount"),
            col("discount_percent"),
            col("net_amount"),
            current_timestamp().alias("created_at")
        )
        
        fact_sales.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", pg_url) \
            .option("dbtable", "public.fact_sales") \
            .options(**pg_props) \
            .save()
        
        logger.info(f"✓ Loaded {fact_sales.count()} sales facts")
        
        # =====================================================================
        # Compute Aggregations
        # =====================================================================
        
        logger.info("Computing aggregation tables...")
        
        # Daily sales summary
        daily_summary = orders_df.groupBy("order_date") \
            .agg(
                col("total_amount").cast("decimal").sum().alias("total_sales"),
                col("quantity").sum().alias("total_quantity"),
                col("order_id").count().alias("transaction_count"),
                col("customer_id").countDistinct().alias("unique_customers")
            ) \
            .withColumn("avg_transaction_value", col("total_sales") / col("transaction_count"))
        
        logger.info(f"✓ Computed daily summary for {daily_summary.count()} days")
        
        # =====================================================================
        # Final Summary
        # =====================================================================
        
        logger.info("=" * 80)
        logger.info(f"📊 Gold Layer Aggregation Metrics:")
        logger.info(f"   - Customers Loaded: {dim_customers.count()}")
        logger.info(f"   - Products Loaded: {dim_products.count()}")
        logger.info(f"   - Sales Facts Loaded: {fact_sales.count()}")
        logger.info(f"   - Daily Summaries: {daily_summary.count()}")
        logger.info("=" * 80)
        logger.info("✅ Gold Layer Aggregation Completed Successfully")
        
        spark.stop()
        return 0
        
    except Exception as e:
        logger.error(f"❌ Error during gold layer aggregation: {str(e)}")
        logger.exception(e)
        return 1

if __name__ == "__main__":
    exit_code = aggregate_to_warehouse()
    sys.exit(exit_code)
