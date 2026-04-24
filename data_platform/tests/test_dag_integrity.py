from airflow.models import DagBag


def test_dag_bag_has_no_import_errors():
    dag_bag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    assert not dag_bag.import_errors, dag_bag.import_errors


def test_expected_dags_exist():
    dag_bag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    assert "data_platform_medallion_pipeline" in dag_bag.dags
    assert "ops_connection_smoke_test" in dag_bag.dags
