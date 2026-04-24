from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from pustaka.tasks import bronze_tasks, gold_task, silver_tasks
from pustaka.runtime_config import get_platform_env
from pustaka.config import POSTGRES_WAREHOUSE_CONN

DATASET_NAMES = ("customers", "products", "orders")
LAYER_ORDER = {"bronze": 1, "silver": 2, "gold": 3, "all": 3}


def log_start(**context):
    print(f"Starting pipeline run for {context['dag_run'].run_id}")


def log_end():
    print("Pipeline completed")


def _normalize_datasets(raw_value):
    if raw_value in (None, "", []):
        return ["all"]
    if isinstance(raw_value, str):
        values = [item.strip() for item in raw_value.split(",") if item.strip()]
    else:
        values = list(raw_value)
    if not values:
        return ["all"]
    if "all" in values:
        return ["all"]
    return sorted(set(values))


def _resolved_window(context):
    interval_start = context.get("data_interval_start") or context["logical_date"]
    interval_end = context.get("data_interval_end") or interval_start
    return interval_start.date().isoformat(), interval_end.date().isoformat()


def validate_runtime_params(**context):
    params = context["params"]
    start_date, end_date = _resolved_window(context)

    validated = {
        "run_mode": params.get("run_mode", "incremental"),
        "start_date": params.get("start_date") or start_date,
        "end_date": params.get("end_date") or end_date,
        "datasets": _normalize_datasets(params.get("datasets")),
        "skip_quality_checks": bool(params.get("skip_quality_checks", False)),
        "target_layer": params.get("target_layer", "all"),
    }

    if validated["run_mode"] not in {"incremental", "full_refresh"}:
        raise ValueError("run_mode must be 'incremental' or 'full_refresh'")

    if validated["target_layer"] not in {"bronze", "silver", "gold", "all"}:
        raise ValueError("target_layer must be bronze, silver, gold, or all")

    invalid_datasets = [item for item in validated["datasets"] if item not in {"all", *DATASET_NAMES}]
    if invalid_datasets:
        raise ValueError(f"Unsupported datasets: {invalid_datasets}")

    if validated["start_date"] > validated["end_date"]:
        raise ValueError("start_date cannot be after end_date")

    if validated["target_layer"] in {"gold", "all"} and validated["datasets"] != ["all"]:
        raise ValueError("gold/all runs require datasets=['all'] so the warehouse stays consistent")

    print(f"Validated runtime config: {validated}")
    return validated


def should_run_dataset(dataset, required_layer, **context):
    runtime = context["ti"].xcom_pull(task_ids="validate_runtime_params")
    datasets = runtime["datasets"]
    allowed_dataset = "all" in datasets or dataset in datasets
    allowed_layer = LAYER_ORDER[required_layer] <= LAYER_ORDER[runtime["target_layer"]]
    if not (allowed_dataset and allowed_layer):
        print(f"Skipping dataset={dataset} layer={required_layer} for runtime={runtime}")
    return allowed_dataset and allowed_layer


def should_run_gold(**context):
    runtime = context["ti"].xcom_pull(task_ids="validate_runtime_params")
    return LAYER_ORDER["gold"] <= LAYER_ORDER[runtime["target_layer"]]


def run_quality_checks(**context):
    runtime = context["ti"].xcom_pull(task_ids="validate_runtime_params")
    if runtime["skip_quality_checks"]:
        print("Skipping quality checks by request")
        return
    print(f"Running lightweight quality checks for datasets={runtime['datasets']}")


default_args = {
    "owner": "data_engineering",
    "retries": 2 if get_platform_env() == "production" else 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_platform_medallion_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    params={
        "run_mode": Param("incremental", type="string", enum=["incremental", "full_refresh"]),
        "start_date": Param("", type=["null", "string"]),
        "end_date": Param("", type=["null", "string"]),
        "datasets": Param(["all"], type=["array", "string"]),
        "skip_quality_checks": Param(False, type="boolean"),
        "target_layer": Param("all", type="string", enum=["bronze", "silver", "gold", "all"]),
    },
    tags=["medallion", get_platform_env()],
) as dag:
    start = PythonOperator(task_id="start", python_callable=log_start)

    validate_params_task = PythonOperator(
        task_id="validate_runtime_params",
        python_callable=validate_runtime_params,
    )

    bronze = bronze_tasks()
    silver = silver_tasks()
    gold = gold_task()

    customer_bronze_gate = ShortCircuitOperator(
        task_id="gate_bronze_customers",
        python_callable=should_run_dataset,
        op_kwargs={"dataset": "customers", "required_layer": "bronze"},
    )
    product_bronze_gate = ShortCircuitOperator(
        task_id="gate_bronze_products",
        python_callable=should_run_dataset,
        op_kwargs={"dataset": "products", "required_layer": "bronze"},
    )
    order_bronze_gate = ShortCircuitOperator(
        task_id="gate_bronze_orders",
        python_callable=should_run_dataset,
        op_kwargs={"dataset": "orders", "required_layer": "bronze"},
    )

    customer_silver_gate = ShortCircuitOperator(
        task_id="gate_silver_customers",
        python_callable=should_run_dataset,
        op_kwargs={"dataset": "customers", "required_layer": "silver"},
    )
    product_silver_gate = ShortCircuitOperator(
        task_id="gate_silver_products",
        python_callable=should_run_dataset,
        op_kwargs={"dataset": "products", "required_layer": "silver"},
    )
    order_silver_gate = ShortCircuitOperator(
        task_id="gate_silver_orders",
        python_callable=should_run_dataset,
        op_kwargs={"dataset": "orders", "required_layer": "silver"},
    )

    quality_checks = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks,
        trigger_rule="none_failed_min_one_success",
    )

    gold_gate = ShortCircuitOperator(task_id="gate_gold", python_callable=should_run_gold)

    compute_summary = PostgresOperator(
        task_id="compute_summary",
        sql="/opt/airflow/dags/sql/compute_daily_sales_summary.sql",
        postgres_conn_id=POSTGRES_WAREHOUSE_CONN,
    )
    compute_product_performance = PostgresOperator(
        task_id="compute_product_performance",
        sql="/opt/airflow/dags/sql/compute_product_performance.sql",
        postgres_conn_id=POSTGRES_WAREHOUSE_CONN,
    )
    compute_customer_lifetime_value = PostgresOperator(
        task_id="compute_customer_lifetime_value",
        sql="/opt/airflow/dags/sql/compute_customer_lifetime_value.sql",
        postgres_conn_id=POSTGRES_WAREHOUSE_CONN,
    )

    end = PythonOperator(task_id="end", python_callable=log_end, trigger_rule="all_done")

    start >> validate_params_task

    validate_params_task >> customer_bronze_gate >> bronze["customers"] >> customer_silver_gate >> silver["customers"]
    validate_params_task >> product_bronze_gate >> bronze["products"] >> product_silver_gate >> silver["products"]
    validate_params_task >> order_bronze_gate >> bronze["orders"] >> order_silver_gate >> silver["orders"]

    [silver["customers"], silver["products"], silver["orders"]] >> quality_checks >> gold_gate >> gold
    gold >> compute_summary >> compute_product_performance >> compute_customer_lifetime_value >> end

    [bronze["customers"], bronze["products"], bronze["orders"]] >> end
    [silver["customers"], silver["products"], silver["orders"]] >> end
