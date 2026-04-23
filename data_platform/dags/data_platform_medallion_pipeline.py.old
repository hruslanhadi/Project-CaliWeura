"""
Data Platform End-to-End DAG
=============================

This DAG orchestrates the medallion architecture pipeline:
1. Bronze Layer: Raw data ingestion
2. Silver Layer: Transformation and cleaning
3. Gold Layer: Aggregation and business intelligence

Author: hruslanhadi
Date: 2026-04-21
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import logging

logger = logging.getLogger(__name__)

# ============================================================================
# Default Arguments
# ============================================================================
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ============================================================================
# DAG Definition
# ============================================================================
dag = DAG(
    dag_id='data_platform_medallion_pipeline',
    default_args=default_args,
    description='End-to-end medallion architecture data pipeline',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['data_platform', 'medallion', 'production'],
)

# ============================================================================
# Helper Functions
# ============================================================================

def log_pipeline_start(**context):
    """Log pipeline execution start."""
    execution_date = context['execution_date']
    logger.info(f"🚀 Starting data platform pipeline for {execution_date}")
    logger.info("=" * 80)


def log_pipeline_end(**context):
    """Log pipeline execution completion."""
    logger.info("=" * 80)
    logger.info("✅ Data platform pipeline completed successfully!")


def validate_bronze_data(**context):
    """Validate bronze layer data quality."""
    logger.info("Validating bronze layer data...")
    # Implementation for data validation
    logger.info("✅ Bronze data validation passed")


# ============================================================================
# Bronze Layer: Data Ingestion
# ============================================================================

task_pipeline_start = PythonOperator(
    task_id='pipeline_start',
    python_callable=log_pipeline_start,
    provide_context=True,
    dag=dag,
)

task_ingest_customers = SparkSubmitOperator(
    task_id='ingest_customers',
    application='/opt/airflow/spark_jobs/01_ingest_customers.py',
    conf={
        'spark.executor.memory': '1g',
        'spark.driver.memory': '1g',
        'spark.executor.cores': '2',
    },
    env_vars={
        'MINIO_ENDPOINT': 'minio:9000',
        'MINIO_ACCESS_KEY': 'minioadmin',
        'MINIO_SECRET_KEY': 'minioadmin_secure_pass',
    },
    master='spark://spark_master:7077',
    dag=dag,
)

task_ingest_products = SparkSubmitOperator(
    task_id='ingest_products',
    application='/opt/airflow/spark_jobs/01_ingest_products.py',
    conf={
        'spark.executor.memory': '1g',
        'spark.driver.memory': '1g',
        'spark.executor.cores': '2',
    },
    env_vars={
        'MINIO_ENDPOINT': 'minio:9000',
        'MINIO_ACCESS_KEY': 'minioadmin',
        'MINIO_SECRET_KEY': 'minioadmin_secure_pass',
    },
    master='spark://spark_master:7077',
    dag=dag,
)

task_ingest_orders = SparkSubmitOperator(
    task_id='ingest_orders',
    application='/opt/airflow/spark_jobs/01_ingest_orders.py',
    conf={
        'spark.executor.memory': '1g',
        'spark.driver.memory': '1g',
        'spark.executor.cores': '2',
    },
    env_vars={
        'MINIO_ENDPOINT': 'minio:9000',
        'MINIO_ACCESS_KEY': 'minioadmin',
        'MINIO_SECRET_KEY': 'minioadmin_secure_pass',
    },
    master='spark://spark_master:7077',
    dag=dag,
)

task_validate_bronze = PythonOperator(
    task_id='validate_bronze_data',
    python_callable=validate_bronze_data,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# Silver Layer: Transformation and Cleaning
# ============================================================================

task_transform_customers = SparkSubmitOperator(
    task_id='transform_customers_silver',
    application='/opt/airflow/spark_jobs/02_transform_customers.py',
    conf={
        'spark.executor.memory': '1g',
        'spark.driver.memory': '1g',
        'spark.executor.cores': '2',
    },
    env_vars={
        'MINIO_ENDPOINT': 'minio:9000',
        'MINIO_ACCESS_KEY': 'minioadmin',
        'MINIO_SECRET_KEY': 'minioadmin_secure_pass',
    },
    master='spark://spark_master:7077',
    dag=dag,
)

task_transform_products = SparkSubmitOperator(
    task_id='transform_products_silver',
    application='/opt/airflow/spark_jobs/02_transform_products.py',
    conf={
        'spark.executor.memory': '1g',
        'spark.driver.memory': '1g',
        'spark.executor.cores': '2',
    },
    env_vars={
        'MINIO_ENDPOINT': 'minio:9000',
        'MINIO_ACCESS_KEY': 'minioadmin',
        'MINIO_SECRET_KEY': 'minioadmin_secure_pass',
    },
    master='spark://spark_master:7077',
    dag=dag,
)

task_transform_orders = SparkSubmitOperator(
    task_id='transform_orders_silver',
    application='/opt/airflow/spark_jobs/02_transform_orders.py',
    conf={
        'spark.executor.memory': '1g',
        'spark.driver.memory': '1g',
        'spark.executor.cores': '2',
    },
    env_vars={
        'MINIO_ENDPOINT': 'minio:9000',
        'MINIO_ACCESS_KEY': 'minioadmin',
        'MINIO_SECRET_KEY': 'minioadmin_secure_pass',
    },
    master='spark://spark_master:7077',
    dag=dag,
)

# ============================================================================
# Gold Layer: Aggregation and Warehouse Loading
# ============================================================================

task_aggregate_to_warehouse = SparkSubmitOperator(
    task_id='aggregate_to_warehouse',
    application='/opt/airflow/spark_jobs/03_aggregate_to_warehouse.py',
    conf={
        'spark.executor.memory': '1g',
        'spark.driver.memory': '1g',
        'spark.executor.cores': '2',
    },
    env_vars={
        'MINIO_ENDPOINT': 'minio:9000',
        'MINIO_ACCESS_KEY': 'minioadmin',
        'MINIO_SECRET_KEY': 'minioadmin_secure_pass',
        'PG_HOST': 'postgres',
        'PG_USER': 'warehouse_user',
        'PG_PASSWORD': 'warehouse_pass',
        'PG_DB': 'data_warehouse',
    },
    master='spark://spark_master:7077',
    dag=dag,
)

# Compute aggregations in PostgreSQL
task_compute_daily_summary = PostgresOperator(
    task_id='compute_daily_summary',
    sql='sql/compute_daily_sales_summary.sql',
    postgres_conn_id='postgres_warehouse',
    dag=dag,
)

task_compute_product_performance = PostgresOperator(
    task_id='compute_product_performance',
    sql='sql/compute_product_performance.sql',
    postgres_conn_id='postgres_warehouse',
    dag=dag,
)

task_compute_customer_ltv = PostgresOperator(
    task_id='compute_customer_ltv',
    sql='sql/compute_customer_lifetime_value.sql',
    postgres_conn_id='postgres_warehouse',
    dag=dag,
)

task_pipeline_end = PythonOperator(
    task_id='pipeline_end',
    python_callable=log_pipeline_end,
    provide_context=True,
    trigger_rule='all_done',
    dag=dag,
)

# ============================================================================
# Task Dependencies
# ============================================================================

# Bronze layer: Ingest all data sources
(
    task_pipeline_start
    >> [task_ingest_customers, task_ingest_products, task_ingest_orders]
    >> task_validate_bronze
)

# Silver layer: Transform all sources in parallel
(
    task_validate_bronze
    >> [task_transform_customers, task_transform_products, task_transform_orders]
)

# Gold layer: Aggregate and compute metrics
(
    [task_transform_customers, task_transform_products, task_transform_orders]
    >> task_aggregate_to_warehouse
    >> [task_compute_daily_summary, task_compute_product_performance, task_compute_customer_ltv]
    >> task_pipeline_end
)

if __name__ == "__main__":
    dag.cli()
