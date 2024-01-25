import logging

import internal_unit_testing
import pytest
from airflow.models import DagBag

from data_zaad.dags import sales_consolidate_on_bq_dag


@pytest.fixture()
def dagbag():
    return  DagBag(dag_folder='data_zaad/dags/',include_examples=False)

def test_dag_loaded(dagbag):
    dag =  dagbag.get_dag(dag_id="sales_consolidate_on_bq_dag")
    assert dagbag.import_errors == {}
    assert len(dag.tasks) == 8

## This not the best way to test
# def test_dag_import():
#     internal_unit_testing.assert_has_valid_dag(sales_consolidate_on_bq_dag)

