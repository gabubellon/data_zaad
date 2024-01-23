import os
from datetime import timedelta

import airflow
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import \
    LocalFilesystemToGCSOperator

from data_zaad.libs import utils_lib

DOC_MD_DAG = """
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
    "owner": "Sales Team",
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


@dag(
    dag_id="sales_load_xlsx_to_csv_gcs_dag",
    default_args=default_args,
    description="Load Sales Files to GCS",
    schedule=timedelta(days=1),
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    doc_md=DOC_MD_DAG,
)
def sales_load_xlsx_to_csv_gcs():
    """DAG function to  sales_load_xlsx_to_csv_gcs"""

    @task(task_id="delete_sales_files_folder")
    def delete_sales_files_folder():
        utils_lib.delete_folder(sales_folder)

    upload_sales_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_sales_csv_to_gcs",
        src=os.path.join(sales_folder, "csv", "*.csv"),
        dst="data/sales/input/",
        bucket="data-zaad",
        gcp_conn_id="google_cloud",
    )

    download_tasks = []
    for file in sales_files:
        file_id = file.get("file_id")
        url = file.get("url").replace(" ", "%20")
        xlsx_file = os.path.join(sales_folder, "xlsx", f"{file_id}.xlsx")
        csv_file = os.path.join(sales_folder, "csv", f"{file_id}.csv")

        @task(task_id=f"donwload_sales_xlsx_{file_id}")
        def donwload_sales_xlsx(xlsx_file, url):
            utils_lib.donwload_file(xlsx_file, url)

        @task(task_id=f"transform_xlsx_to_csv_{file_id}")
        def transform_xlsx_to_csv(xlsx_file, csv_file):
            utils_lib.transform_xlsx_to_csv(xlsx_file, csv_file)

        download_tasks.append(
            donwload_sales_xlsx(xlsx_file, url)
            >> transform_xlsx_to_csv(xlsx_file, csv_file)
        )

    (download_tasks >> upload_sales_csv_to_gcs >> delete_sales_files_folder())


sales_load_xlsx_to_csv_gcs()
