CREATE TABLE `gb-data-zaad.sales.sales_raw_data` (
  id_marca INT64 OPTIONS(description = "id da Marca"),
  marca STRING OPTIONS(description = "Nome da Marca"),
  id_linha INT64 OPTIONS(description = "Id da Linha"),
  linha STRING OPTIONS(description = "Nome da Linha"),
  data_venda DATE OPTIONS(description = "data da venda"),
  qtd_venda FLOAT64 OPTIONS(description = "Quantidade de Vendas"),
  csv_file STRING OPTIONS(description = "Arquivo de input")
) PARTITION BY DATE_TRUNC(data_venda, MONTH);
CREATE TABLE `gb-data-zaad.sales.sales_by_line_month` (
  mes_vendas DATE,
  id_linha INT64,
  linha STRING,
  total_qtd_vendas FLOAT64
);
CREATE TABLE `gb-data-zaad.sales.sales_by_brand_line` (
  id_marca INT64,
  marca STRING,
  id_linha INT64,
  linha STRING,
  total_qtd_vendas FLOAT64
);
CREATE TABLE `gb-data-zaad.sales.sales_by_month_year` (mes_vendas DATE, total_qtd_vendas FLOAT64);
CREATE TABLE `gb-data-zaad.sales.sales_by_brand_month` (
  mes_vendas DATE,
  id_marca INT64,
  marca STRING,
  total_qtd_vendas FLOAT64
);