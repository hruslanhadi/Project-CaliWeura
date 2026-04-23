# dags/data_platform/pipeline.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.baseoperator import cross_downstream
from airflow.models.baseoperator import chain
from datetime import timedelta
from airflow.utils.dates import days_ago

from pustaka.tasks import bronze_tasks, silver_tasks, gold_task
from pustaka.config import POSTGRES_WAREHOUSE_CONN


def log_start(**context):
    print(f"Starting pipeline: {context['execution_date']}")


def log_end(**context):
    print("Pipeline completed successfully")


default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_platform_medallion_pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["medallion", "production"],
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=log_start,
    )

    bronze = bronze_tasks()
    silver = silver_tasks()
    gold = gold_task()

    compute_summary = PostgresOperator(
        task_id="compute_summary",
        sql="/opt/airflow/configs/sql/compute_daily_sales_summary.sql",
        postgres_conn_id=POSTGRES_WAREHOUSE_CONN,
    )

    end = PythonOperator(
        task_id="end",
        python_callable=log_end,
        trigger_rule="all_done",
    )

    # Dependencies
    start >> bronze
    chain(bronze, silver)
    chain(silver, gold)
    gold >> compute_summary >> end