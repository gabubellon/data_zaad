import pytest
from airflow.models import DagBag


@pytest.fixture()
def dagbag():
    return  DagBag(dag_folder='sales_dags/',include_examples=False)

def test_dag_loaded(dagbag):
    dag =  dagbag.get_dag(dag_id="sales_consolidate_on_bq_dag")
    assert dagbag.import_errors == {}
    assert len(dag.tasks) == 8



