"""A DAG to consolidade sales data"""

from datetime import timedelta

import airflow
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryInsertJobOperator

DOC_MD_DAG = """
### Consolidate Sales data on BQ

DAG responsável pela consolidação dos dados de vendas tendo como fonte a tabela `sales_raw_data`

* Tabela `sales_by_month_year`: Consolidado de vendas por ano e mês;
* Tabela `sales_by_brand_line`: Consolidado de vendas por marca e linha;
* Tabela `sales_by_brand_month`: Consolidado de vendas por marca, ano e mês; 
* Tabela `sales_by_line_month`: Consolidado de vendas por linha, ano e mês;

"""

default_args = {
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "owner": "Sales Team",
}

sales_queries = [
    {
        "table_name": "gb-data-zaad.sales.sales_by_month_year",
        "insert_query": """
SELECT
   DATE_TRUNC(data_venda, MONTH) as mes_vendas,
   sum(qtd_venda) as total_qtd_vendas
from gb-data-zaad.sales.sales_raw_data
group by mes_vendas
""",
    },
    {
        "table_name": "gb-data-zaad.sales.sales_by_brand_line",
        "insert_query": """
SELECT
   id_marca,
   marca,
   id_linha,
   linha,
   sum(qtd_venda) as total_qtd_vendas
from gb-data-zaad.sales.sales_raw_data
group by 
  id_marca,
   marca,
   id_linha,
   linha
""",
    },
    {
        "table_name": "gb-data-zaad.sales.sales_by_brand_month",
        "insert_query": """
SELECT
   DATE_TRUNC(data_venda, MONTH) as mes_vendas,
   id_marca,
   marca,
   sum(qtd_venda) as total_qtd_vendas
from gb-data-zaad.sales.sales_raw_data
group by 
   mes_vendas,
   id_marca,
   marca
""",
    },
    {
        "table_name": "gb-data-zaad.sales.sales_by_line_month",
        "insert_query": """
SELECT
   DATE_TRUNC(data_venda, MONTH) as mes_vendas,
   id_linha,
   linha,
   sum(qtd_venda) as total_qtd_vendas
from gb-data-zaad.sales.sales_raw_data
group by 
   mes_vendas,
   id_linha,
   linha
""",
    },
]

TRUNCATE_TEMPLATE = "TRUNCATE TABLE {table_name}"
INSERT_TEMPLATE = """
        INSERT INTO {table_name}
        {insert_query}
    """

with DAG(
    "sales_consolidate_on_bq_dag",
    default_args=default_args,
    description="Consolidaye Sales data on BQ",
    schedule=timedelta(days=1),
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    doc_md=DOC_MD_DAG,
) as dag:
    for query in sales_queries:
        table_name = query.get("table_name")
        insert_query = query.get("insert_query")

        truncate_table = BigQueryInsertJobOperator(
            task_id=f"truncate_table_{table_name}",
            configuration={
                "query": {
                    "query": TRUNCATE_TEMPLATE.format(table_name=table_name),
                    "useLegacySql": False,
                    "priority": "BATCH",
                }
            },
            gcp_conn_id="google_cloud",
        )

        insert_table = BigQueryInsertJobOperator(
            task_id=f"insert_table_{table_name}",
            configuration={
                "query": {
                    "query": INSERT_TEMPLATE.format(
                        table_name=table_name, insert_query=insert_query
                    ),
                    "useLegacySql": False,
                    "priority": "BATCH",
                }
            },
            gcp_conn_id="google_cloud",
        )

        truncate_table >> insert_table
