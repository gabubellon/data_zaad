import internal_unit_testing

from data_zaad.dags import airflow_monitoring


def test_dag_import():
    internal_unit_testing.assert_has_valid_dag(airflow_monitoring)
    