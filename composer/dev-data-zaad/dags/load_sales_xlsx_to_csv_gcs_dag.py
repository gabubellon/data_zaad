import logging
import os
import shutil
from datetime import timedelta
from urllib.request import urlretrieve

import airflow
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import \
    LocalFilesystemToGCSOperator

doc_md_DAG = """
### Load Sales Excel Data to CVS on Google Cloud Storage

DAG responsável pela carga das planilhas de vendas (xlsx) para o Google Cloud Storage em format CSV

Passos:
- Criação das pastas de download
- Donwload dos arquivos em excel
- Transformação dos arquivos em csv
- Upload dos aqruivos pra o bucket `data-zaad` no gcs
- Exclusão dos arquivos locais
"""

default_args = {
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "owner": "DE Team",
}

sales_files = [
    {
        "file_id": "sales_2017",
        "url": "https://github.com/gabubellon/data_zaad/raw/main/resources/Base 2017.xlsx",
    },
    {
        "file_id": "sales_2018",
        "url": "https://github.com/gabubellon/data_zaad/raw/main/resources/Base_2018.xlsx",
    },
    {
        "file_id": "sales_2019",
        "url": "https://github.com/gabubellon/data_zaad/raw/main/resources/Base_2019.xlsx",
    },
]

sales_folder = "./sales_data/"


def create_folder(folder_name):
    os.makedirs(name=folder_name, exist_ok=True)


def delete_folder(folder_name):
    shutil.rmtree(folder_name)


def donwload_file(file_name, url):
    saved_file = urlretrieve(url, file_name)
    logging.info(saved_file)


def transform_xlsx_to_csv(xlsx_file, csv_file):
    data_frame = pd.read_excel(xlsx_file)
    data_frame["csv_file"] = os.path.basename(csv_file)
    data_frame.columns = [c.lower() for c in  data_frame.columns]
    data_frame.to_csv(csv_file, index=False)


with DAG(
    "load_sales_xlsx_to_csv_gcs_dag",
    default_args=default_args,
    description="Load Sales Data to BQ",
    schedule=timedelta(days=1),
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    doc_md=doc_md_DAG

) as dag:
    create_sales_files_folder = PythonOperator(
        task_id="create_sales_files_folder",
        python_callable=create_folder,
        op_kwargs={"folder_name": sales_folder},
    )

    create_sales_xlsx_folder = PythonOperator(
        task_id="create_sales_xlsx_folder",
        python_callable=create_folder,
        op_kwargs={"folder_name": os.path.join(sales_folder, "xlsx")},
    )

    create_sales_csv_folder = PythonOperator(
        task_id="create_sales_csv_folder",
        python_callable=create_folder,
        op_kwargs={"folder_name": os.path.join(sales_folder, "csv")},
    )

    delete_sales_files_folder = PythonOperator(
        task_id="delete_sales_files_folder",
        python_callable=delete_folder,
        op_kwargs={"folder_name": sales_folder},
    )

    upload_sales_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_sales_csv_to_gcs",
        src=os.path.join(sales_folder, "csv", "*.csv"),
        dst="data/sales/input/",
        bucket="data-zaad",
        gcp_conn_id="google_cloud",
    )

    for file in sales_files:
        file_id = file.get("file_id")
        url = file.get("url").replace(" ", "%20")
        xlsx_file = os.path.join(sales_folder, "xlsx", f"{file_id}.xlsx")
        csv_file = os.path.join(sales_folder, "csv", f"{file_id}.csv")

        download_params = {"file_name": xlsx_file, "url": url}

        download_operator = PythonOperator(
            task_id=f"donwload_sales_xlsx_{file_id}",
            python_callable=donwload_file,
            op_kwargs=download_params,
        )

        csv_params = {"xlsx_file": xlsx_file, "csv_file": csv_file}

        csv_operator = PythonOperator(
            task_id=f"transform_xlsx_to_csv_{file_id}",
            python_callable=transform_xlsx_to_csv,
            op_kwargs=csv_params,
        )

        create_sales_files_folder >> [create_sales_xlsx_folder, create_sales_csv_folder]
        [create_sales_xlsx_folder, create_sales_csv_folder] >> download_operator
        download_operator >> csv_operator
        csv_operator >> upload_sales_csv_to_gcs
        upload_sales_csv_to_gcs >> delete_sales_files_folder
