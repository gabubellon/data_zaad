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
- Cria a tabela temporária `sales_raw_data_temp` para carga dos dados 
- Carrega dos os arquivos do gcs na tabela `sales_raw_data_temp`
- Exclui dados antigos da tabela `sales_raw_data` 
- Insere novos dados da tabela `sales_raw_data_temp` para a tabela `sales_raw_data`
- Exclui a `sales_raw_data_temp` 
"""

default_args = {
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "owner": "Sales Team",
}

clone_sales_raw_query = "CREATE OR REPLACE TABLE gb-data-zaad.sales.sales_raw_data_yyyy as select * from gb-data-zaad.sales.sales_raw_data limit 0;"
clean_sales_raw_query = """
    delete from gb-data-zaad.sales.sales_raw_data
    where csv_file in (
        Select 
            distinct
            csv_file
        from gb-data-zaad.sales.sales_raw_data_temp
    )
"""
insert_sales_raw_query = """
    insert into gb-data-zaad.sales.sales_raw_data
    Select 
    *
    from gb-data-zaad.sales.sales_raw_data_temp
"""
drop_sales_raw_query = "DROP TABLE IF EXISTS gb-data-zaad.sales.sales_raw_data_temp"

with DAG(
    "sales_load_csv_gcs_to_bq_dag",
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
        source_object="data/sales/input/",
        destination_object="data/sales/import/",
        move_object=True,
        match_glob="**/*.csv",
        gcp_conn_id="google_cloud",
    )

    move_from_import_to_done = GCSToGCSOperator(
        task_id="move_from_import_to_done",
        source_bucket="data-zaad",
        source_object="data/sales/import/",
        destination_object="data/sales/done/",
        move_object=True,
        match_glob="**/*.csv",
        gcp_conn_id="google_cloud",
    )

    clone_sales_raw_table = BigQueryInsertJobOperator(
        task_id="clone_sales_raw_table",
        configuration={
            "query": {
                "query": clone_sales_raw_query,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        gcp_conn_id="google_cloud",
    )

    load_csv_to_sales_temp_bq = GCSToBigQueryOperator(
        task_id="load_csv_to_sales_temp_bq",
        bucket="data-zaad",
        source_objects=["data/sales/import/*.csv"],
        destination_project_dataset_table="gb-data-zaad.sales.sales_raw_data_temp",
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id="google_cloud",
    )

    clean_sales_raw = BigQueryInsertJobOperator(
        task_id="clean_sales_raw_query",
        configuration={
            "query": {
                "query": clean_sales_raw_query,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        gcp_conn_id="google_cloud",
    )

    insert_sales_raw = BigQueryInsertJobOperator(
        task_id="insert_sales_raw",
        configuration={
            "query": {
                "query": insert_sales_raw_query,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        gcp_conn_id="google_cloud",
    )

    drop_sales_raw_temp_table = BigQueryInsertJobOperator(
        task_id="drop_sales_raw_temp_table",
        configuration={
            "query": {
                "query": drop_sales_raw_query,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        gcp_conn_id="google_cloud",
    )

    (
        move_from_input_to_import
        >> clone_sales_raw_table
        >> load_csv_to_sales_temp_bq
        >> clean_sales_raw
        >> insert_sales_raw
        >> drop_sales_raw_temp_table
        >> move_from_import_to_done
    )
