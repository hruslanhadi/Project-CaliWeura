#!/usr/bin/env python3
"""
Data Quality Validation Script
================================

Validates data quality metrics across the pipeline

Author: hruslanhadi
Date: 2026-04-21
"""

import logging
import os

import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_record_counts():
    """Check record counts in each layer."""
    logger.info("🔍 Checking record counts across warehouse tables...")
    conn = psycopg2.connect(
        host=os.getenv("PG_WAREHOUSE_HOST", "postgres"),
        port=os.getenv("PG_WAREHOUSE_PORT", "5432"),
        dbname=os.getenv("PG_WAREHOUSE_DB", "data_warehouse"),
        user=os.getenv("PG_WAREHOUSE_USER", "warehouse_user"),
        password=os.getenv("PG_WAREHOUSE_PASSWORD", "warehouse_dev_pass"),
    )
    with conn:
        with conn.cursor() as cursor:
            for table_name in ("dim_customers", "dim_products", "fact_sales"):
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                logger.info("   %s rows=%s", table_name, count)
    logger.info("✅ Record count validation passed")

def check_data_quality():
    """Validate data quality metrics."""
    logger.info("🔍 Checking data quality metrics...")
    conn = psycopg2.connect(
        host=os.getenv("PG_WAREHOUSE_HOST", "postgres"),
        port=os.getenv("PG_WAREHOUSE_PORT", "5432"),
        dbname=os.getenv("PG_WAREHOUSE_DB", "data_warehouse"),
        user=os.getenv("PG_WAREHOUSE_USER", "warehouse_user"),
        password=os.getenv("PG_WAREHOUSE_PASSWORD", "warehouse_dev_pass"),
    )
    with conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM dim_customers WHERE external_customer_id IS NULL")
            assert cursor.fetchone()[0] == 0
            cursor.execute("SELECT COUNT(*) FROM dim_products WHERE external_product_id IS NULL")
            assert cursor.fetchone()[0] == 0
    logger.info("✅ Data quality validation passed")

def check_schema_consistency():
    """Validate schema consistency."""
    logger.info("🔍 Checking schema consistency...")
    conn = psycopg2.connect(
        host=os.getenv("PG_WAREHOUSE_HOST", "postgres"),
        port=os.getenv("PG_WAREHOUSE_PORT", "5432"),
        dbname=os.getenv("PG_WAREHOUSE_DB", "data_warehouse"),
        user=os.getenv("PG_WAREHOUSE_USER", "warehouse_user"),
        password=os.getenv("PG_WAREHOUSE_PASSWORD", "warehouse_dev_pass"),
    )
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'fact_sales'
                ORDER BY ordinal_position
                """
            )
            columns = [row[0] for row in cursor.fetchall()]
            expected = [
                "sale_id",
                "customer_id",
                "product_id",
                "date_id",
                "quantity",
                "unit_price",
                "total_amount",
                "discount_percent",
                "net_amount",
                "order_id",
                "created_at",
            ]
            assert columns == expected, columns
    logger.info("✅ Schema consistency validation passed")

def main():
    """Main validation runner."""
    logger.info("=" * 80)
    logger.info("📊 Data Quality Validation Report")
    logger.info("=" * 80)
    
    check_record_counts()
    check_data_quality()
    check_schema_consistency()
    
    logger.info("=" * 80)
    logger.info("✅ All validations passed!")
    logger.info("=" * 80)

if __name__ == "__main__":
    main()
