from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime

# ----------------------
# Python Test (basic runtime)
# ----------------------
def test_python():
    print("✅ PythonOperator is working")


# ----------------------
# DAG Definition
# ----------------------
with DAG(
    dag_id="test_connections_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["debug", "connection-test"],
) as dag:

    # 1. Test Airflow itself
    test_python_task = PythonOperator(
        task_id="test_python",
        python_callable=test_python,
    )

    # 2. Test Postgres connection
    test_postgres = PostgresOperator(
        task_id="test_postgres",
        postgres_conn_id="postgres_warehouse",  # must exist
        sql="SELECT 1;",
    )

    # 3. Test Spark connection
    test_spark = SparkSubmitOperator(
        task_id="test_spark",
        application="/opt/airflow/spark_jobs/test_spark.py",
        conn_id="spark_default",  # must exist
        conf={
            "spark.master": "spark://spark-master:7077",
        },
        verbose=True,
    )

    # 4. Test MinIO (via Python)
    def test_minio():
        import boto3

        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin_dev_pass",
        )

        buckets = s3.list_buckets()
        print("✅ MinIO buckets:", buckets)

    test_minio_task = PythonOperator(
        task_id="test_minio",
        python_callable=test_minio,
    )

    # ----------------------
    # Dependencies
    # ----------------------
    test_python_task >> test_postgres >> test_minio_task >> test_spark