import internal_unit_testing

from dags import spotify_load_http_to_csv_gcs_dag


def test_dag_import():
    internal_unit_testing.assert_has_valid_dag(spotify_load_http_to_csv_gcs_dag)
    