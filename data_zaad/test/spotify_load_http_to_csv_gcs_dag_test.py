import pytest
from airflow.models import DagBag


@pytest.fixture()
def dagbag():
    return  DagBag(dag_folder='spotify_dags/',include_examples=False)

def test_dag_loaded(dagbag):
    dag =  dagbag.get_dag(dag_id="spotify_load_http_to_csv_gcs_dag")
    assert dagbag.import_errors == {}
    assert len(dag.tasks) == 4
