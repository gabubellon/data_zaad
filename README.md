# Data ZAAD

## Proposta

Esse repositório tem por definição centralizar as consolidação do projeto Data ZAAD do time de dados do GB

### Estrutura

* Atualmeent existe consolidação para os dados de vendas e para os dados de buscas do spotify
* Todos os dados são carregados para um bucket do `gcs` enviados para tabelas no BigQuery, cujo os scripts ddls podem ser encontratos na pastal [sql](./sql/ddl)
* Todos os arquivos locais são excluidos após o envio para bucket do `gcs` 
* Todos os dados no bucket do `gcs`  seguem o fluxo de pastas:
  * `input` dados recem carregados para o `gcs`;
  * `import` dados em processo de importação para o Big Query;
  * `done` dados inseridos com sucessop no Big Query

#### Vendas

* [sales_load_xlsx_to_csv_gcs_dag](dags/sales_load_xlsx_to_csv_gcs_dag.py): DAG que realiza o downlaod dos dados e envia para o bucket `gcs` em formato csv;
* [sales_load_csv_gcs_to_bq_dag](dags/sales_load_csv_gcs_to_bq_dag.py): DAG que realiza a carga dos dados do bucket para a tabela de dados crus no BiqQuery
* [sales_consolidate_on_bq_dag](dags/sales_consolidate_on_bq_dag.py): DAG que realiza as seguintes consolidações dos dados de vendas no BigQuery:
  * Tabela `sales_by_month_year`: Consolidado de vendas por ano e mês;
  * Tabela `sales_by_brand_line`: Consolidado de vendas por marca e linha;
  * Tabela `sales_by_brand_month`: Consolidado de vendas por marca, ano e mês;
  * Tabela `sales_by_line_month`: Consolidado de vendas por linha, ano e mês;

#### Spotify

* [spotify_load_http_to_csv_gcs_dag](dags/spotify_load_http_to_csv_gcs_dag.py): DAG que realiza a pesquisa por todos os programas do DataHackers e todos os episódios do podcast DataHackers
* [spotify_load_csv_gcs_to_bq_dag](dags/spotify_load_csv_gcs_to_bq_dag.py): DAG que realiza as seguintes consolidações dos dados de spotify no BigQuery:
  * Tabela `data_hackers_search`: Consolidado de vendas por ano e mês;
  * Tabela `data_hackers_search`: Consolidado de vendas por marca e linha;
  * Tabela `data_hackers_episodes_gb`: Consolidado de vendas por marca, ano e mês;

### Desenvolvimento local

Dependencias: Docker

Siga a documentação para instalar o  `composer-dev` [Run Local Airflow Environments](https://cloud.google.com/composer/docs/composer-2/run-local-airflow-environments)

Dentro da pasta do projeto, basta utilizar o commandos `composer-dev start` ou `composer-dev restart` para iniciar o Airflow em modo local

Caso tenha problems com inicialização do banco de dados do Airflow execute o comando `chmod -R ./composer`

Para um melhor desenvolimento local, recomenda-se utilizar um ambiente virtual e instalar as [libs](requirements.txt)
Caso tenha algum problema com a instalação do airflow, utiliza esse script:

```bash
AIRFLOW_VERSION=2.6.3
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

### CI/CD

As pastas `utils` e `cloudbuild` apresentam as configurações necessárias para realizar o deploy e configurar a estrutura de CI/CD no Google Cloud
Para mais detalhes veja [esse](https://medium.com/@amarachi.ogu/implementing-ci-cd-in-cloud-composer-using-cloud-build-and-github-part-2-a721e4ed53da) artigo

### Soluções Utilizadas

**Google Cloud Storage**

![CleanShot 2024-01-22 at 23 14 33@2x](https://github.com/gabubellon/data_zaad/assets/7385097/f6fb1e76-c3df-4ebc-a051-566a16867f8c)

**Google Cloud Big Query**

![CleanShot 2024-01-22 at 23 16 42](https://github.com/gabubellon/data_zaad/assets/7385097/2cfb7cfc-bae0-4fb3-87e0-80ac3d8aad41)

**Google Cloud Composer Airflow**

![CleanShot 2024-01-22 at 23 20 53](https://github.com/gabubellon/data_zaad/assets/7385097/b37ac35a-496a-4539-a68f-960da9d4ae04)



