import pytest
from airflow.models import 

@pytest.fixture()
def dagbag():
    return  DagBag(dag_folder='dags/',include_examples=False)

def test_dag_loaded(dagbag):
    dag =  dagbag.get_dag(dag_id="hello_world")
    assert dagbag.import_errors == {}
    #assert dag is not None
    #assert len(dag.tasks) == 1