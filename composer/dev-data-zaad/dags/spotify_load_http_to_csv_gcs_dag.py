import logging
import os
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import \
    LocalFilesystemToGCSOperator
from lib import utils_lib
from lib.spotify_api_lib import Spotify_API

doc_md_DAG = """
### Load Spotify Data to GCS

DAG responsável por consultar a API do spotify e salvar os dados no GCS

Passos:
- Realizar uma pesquisa pelo termo Data Hackers
- Busca todos os episódios do podcast Data Hackers
- Salva os mesmos em csv local
- Upload dos aqruivos pra o bucket `data-zaad` no gcs
- Exclusão dos arquivos locais
"""

default_args = {
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "owner": "Sales Team",
}


spotify_folder = "./spotify_data"

search_columns = ["id", "name", "description", "total_episodes"]
episodes_columns = [
    "id",
    "name",
    "description",
    "release_date",
    "duration_ms",
    "language",
    "explicit",
    "type",
]

dh_show_id = Variable.get("data_hackers_show_id")
client_id = Variable.get("spotify_client_id")
client_secret = Variable.get("spotify_client_secret")
redirect_uri = Variable.get("spotify_redirect_uri")

def data_hacker_search():   
    dh_search = Spotify_API(client_id,client_secret,redirect_uri).spotify_search("data hacker", 50, "show", 'BR',search_columns)
    utils_lib.transform_dict_to_csv(dh_search, f"{spotify_folder}/dh_search.csv")
    
def data_hacker_episodes():
    dh_episodes = Spotify_API(client_id,client_secret,redirect_uri).spotify_show_episodes(dh_show_id, episodes_columns)
    utils_lib.transform_dict_to_csv(dh_episodes, f"{spotify_folder}/dh_episodes.csv")

with DAG(
    "spotify_load_http_to_csv_gcs_dag",
    default_args=default_args,
    description="Load Sales Files to GCS",
    schedule=timedelta(days=1),
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    doc_md=doc_md_DAG,
) as dag:
    data_hacker_search = PythonOperator(
        task_id="data_hacker_search",
        python_callable=data_hacker_search,
    )

    data_hacker_episodes = PythonOperator(
        task_id="data_hacker_episodes",
        python_callable=data_hacker_episodes,
    )

    upload_sales_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_sales_csv_to_gcs",
        src=f"{spotify_folder}/*.csv",
        dst="data/spotify/input/",
        bucket="data-zaad",
        gcp_conn_id="google_cloud",
    )

    delete_spotify_folder = PythonOperator(
        task_id="delete_sales_files_folder",
        python_callable=utils_lib.delete_folder,
        op_kwargs={"folder_name": spotify_folder},
    )

    (
        (data_hacker_search, data_hacker_episodes)
        >> upload_sales_csv_to_gcs
        >> delete_spotify_folder
    )