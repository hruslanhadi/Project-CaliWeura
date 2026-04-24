from pustaka.task_factory import create_spark_task


def bronze_tasks():
    return {
        "customers": create_spark_task("ingest_customers", "/opt/airflow/spark_jobs/01_ingest_customers.py"),
        "products": create_spark_task("ingest_products", "/opt/airflow/spark_jobs/01_ingest_products.py"),
        "orders": create_spark_task("ingest_orders", "/opt/airflow/spark_jobs/01_ingest_orders.py"),
    }


def silver_tasks():
    return {
        "customers": create_spark_task("transform_customers", "/opt/airflow/spark_jobs/02_transform_customers.py"),
        "products": create_spark_task("transform_products", "/opt/airflow/spark_jobs/02_transform_products.py"),
        "orders": create_spark_task("transform_orders", "/opt/airflow/spark_jobs/02_transform_orders.py"),
    }


def gold_task():
    return create_spark_task(
        "aggregate_to_warehouse",
        "/opt/airflow/spark_jobs/03_aggregate_to_warehouse.py",
        include_warehouse=True,
    )
