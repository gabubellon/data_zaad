import internal_unit_testing

from data_zaad.dags import  sales_load_csv_gcs_to_bq_dag


def test_dag_import():
    internal_unit_testing.assert_has_valid_dag(sales_load_csv_gcs_to_bq_dag)
