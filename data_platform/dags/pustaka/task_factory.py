from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from pustaka.config import SPARK_CONN_ID
from pustaka.runtime_config import get_default_task_env
from pustaka.spark_config import build_spark_conf


def create_spark_task(task_id: str, application: str, include_warehouse: bool = False):
    env_vars = get_default_task_env(include_warehouse=include_warehouse)
    env_vars["PIPELINE_RUNTIME_CONFIG"] = "{{ ti.xcom_pull(task_ids='validate_runtime_params') | tojson }}"

    return SparkSubmitOperator(
        task_id=task_id,
        application=application,
        conn_id=SPARK_CONN_ID,
        conf=build_spark_conf(),
        env_vars=env_vars,
        application_args=[
            "--runtime-config",
            "{{ ti.xcom_pull(task_ids='validate_runtime_params') | tojson }}",
        ],
        verbose=True,
    )
