import os
import subprocess
import unittest

import boto3
import psycopg2
import requests


class TestDataPlatform(unittest.TestCase):
    def test_postgres_connectivity(self):
        conn = psycopg2.connect(
            host=os.getenv("PG_WAREHOUSE_HOST", "postgres"),
            port=os.getenv("PG_WAREHOUSE_PORT", "5432"),
            dbname=os.getenv("PG_WAREHOUSE_DB", "data_warehouse"),
            user=os.getenv("PG_WAREHOUSE_USER", "warehouse_user"),
            password=os.getenv("PG_WAREHOUSE_PASSWORD", "warehouse_dev_pass"),
        )
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT current_database(), current_user")
                db_name, user_name = cursor.fetchone()
                self.assertEqual(db_name, os.getenv("PG_WAREHOUSE_DB", "data_warehouse"))
                self.assertEqual(user_name, os.getenv("PG_WAREHOUSE_USER", "warehouse_user"))
                cursor.execute("SELECT to_regclass('public.dim_customers')")
                self.assertEqual(cursor.fetchone()[0], "dim_customers")

    def test_minio_connectivity(self):
        endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        if not endpoint.startswith("http"):
            endpoint = f"http://{endpoint}"
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin_dev_pass"),
            region_name="us-east-1",
        )
        buckets = {item["Name"] for item in client.list_buckets()["Buckets"]}
        self.assertTrue({"bronze", "silver", "gold"}.issubset(buckets))

    def test_spark_connectivity(self):
        result = subprocess.run(
            ["spark-submit", "/opt/airflow/spark_jobs/test_spark.py"],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)

    def test_airflow_dag_imports(self):
        result = subprocess.run(
            ["airflow", "dags", "list"],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("data_platform_medallion_pipeline", result.stdout)
        self.assertIn("ops_connection_smoke_test", result.stdout)

    def test_spark_master_ui(self):
        response = requests.get("http://spark-master:8080", timeout=10)
        self.assertEqual(response.status_code, 200)


if __name__ == "__main__":
    unittest.main()
