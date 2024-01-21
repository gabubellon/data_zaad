import logging
import os
import shutil
from datetime import timedelta
from urllib.request import urlretrieve

import airflow
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import \
    GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import \
    GCSToGCSOperator

doc_md_DAG = """
### Load CSV data on GCS to BQ

DAG responsável pela carga dos dados dem CSV salvo no GCS para o BigQuery 

Passos:
- Movimentas os arquivos para pasta `import` no gcs
- Carrega dos os arquivos do gcs nas tabela `data_hackers_search` e `data_hackers_episodes`
- Consolida a tabela `data_hackers_episodes_gb`
- Movimentas os arquivos para pasta `done` no gcs
"""

default_args = {
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "owner": "Sales Team",
}

insert_gb_episodes_query = """
  create or replace table gb-data-zaad.spotify.data_hackers_episodes_gb as 
    SELECT 
    *
    from gb-data-zaad.spotify.data_hackers_episodes
    where description like '%Grupo Boticário%' or name like '%Grupo Boticário%'
"""

with DAG(
    "spotify_load_csv_gcs_to_bq_dag",
    default_args=default_args,
    description="Load Sales Data to BQ",
    schedule=timedelta(days=1),
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    doc_md=doc_md_DAG,
) as dag:
    move_from_input_to_import = GCSToGCSOperator(
        task_id="move_from_input_to_import",
        source_bucket="data-zaad",
        source_object="data/spotify/input/",
        destination_object="data/spotify/import/",
        move_object=True,
        match_glob="**/*.csv",
        gcp_conn_id="google_cloud",
    )

    move_from_import_to_done = GCSToGCSOperator(
        task_id="move_from_import_to_done",
        source_bucket="data-zaad",
        source_object="data/spotify/import/",
        destination_object="data/spotify/done/",
        move_object=True,
        match_glob="**/*.csv",
        gcp_conn_id="google_cloud",
    )

    load_dh_search_bq = GCSToBigQueryOperator(
        task_id="load_dh_search_bq",
        bucket="data-zaad",
        source_objects=["data/spotify/import/dh_search.csv"],
        destination_project_dataset_table="gb-data-zaad.spotify.data_hackers_search",
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id="google_cloud",
    )

    load_dh_episode_bq = GCSToBigQueryOperator(
        task_id="load_dh_episode_bq",
        bucket="data-zaad",
        source_objects=["data/spotify/import/dh_episodes.csv"],
        destination_project_dataset_table="gb-data-zaad.spotify.data_hackers_episodes",
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id="google_cloud",
    )

    insert_gb_episodes = BigQueryInsertJobOperator(
        task_id="insert_gb_episodes",
        configuration={
            "query": {
                "query": insert_gb_episodes_query,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        gcp_conn_id="google_cloud",
    )

    (
        move_from_input_to_import
        >> (load_dh_search_bq, load_dh_episode_bq)
        >> move_from_import_to_done
    )
    load_dh_episode_bq >> insert_gb_episodes
