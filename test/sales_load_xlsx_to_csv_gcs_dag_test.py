import internal_unit_testing
import pytest
from airflow.models import DagBag

from data_zaad.dags import sales_load_xlsx_to_csv_gcs_dag


@pytest.fixture()
def dagbag():
    return  DagBag(dag_folder='data_zaad/dags/',include_examples=False)

def test_dag_loaded(dagbag):
    dag =  dagbag.get_dag(dag_id="sales_load_xlsx_to_csv_gcs_dag")
    assert dagbag.import_errors == {}
    assert len(dag.tasks) == 8

def test_dag_import():
    #internal_unit_testing.assert_has_valid_dag(sales_load_xlsx_to_csv_gcs_dag)
    pass
