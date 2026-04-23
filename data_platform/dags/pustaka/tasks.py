# dags/data_platform/tasks.py

from pustaka.task_factory import create_spark_task

# ----------------------
# Bronze
# ----------------------
def bronze_tasks():
    return [
        create_spark_task("ingest_customers", "/opt/airflow/spark_jobs/01_ingest_customers.py"),
        create_spark_task("ingest_products", "/opt/airflow/spark_jobs/01_ingest_products.py"),
        create_spark_task("ingest_orders", "/opt/airflow/spark_jobs/01_ingest_orders.py"),
    ]


# ----------------------
# Silver
# ----------------------
def silver_tasks():
    return [
        create_spark_task("transform_customers", "/opt/airflow/spark_jobs/02_transform_customers.py"),
        create_spark_task("transform_products", "/opt/airflow/spark_jobs/02_transform_products.py"),
        create_spark_task("transform_orders", "/opt/airflow/spark_jobs/02_transform_orders.py"),
    ]


# ----------------------
# Gold
# ----------------------
def gold_task():
    return create_spark_task(
        "aggregate_to_warehouse",
        "/opt/airflow/spark_jobs/03_aggregate_to_warehouse.py",
        extra_env={
            "PG_HOST": "postgres",
            "PG_USER": "warehouse_user",
            "PG_PASSWORD": "warehouse_pass",
            "PG_DB": "data_warehouse",
        },
    )