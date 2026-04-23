# dags/data_platform/task_factory.py

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pustaka.spark_config import build_spark_conf
from pustaka.config import SPARK_CONN_ID


def create_spark_task(task_id: str, application: str, extra_env: dict = None):
    return SparkSubmitOperator(
        task_id=task_id,
        application=application,
        conn_id=SPARK_CONN_ID,
        conf=build_spark_conf(),
        env_vars=extra_env or {},
    )