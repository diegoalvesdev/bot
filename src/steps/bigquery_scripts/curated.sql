-- 1 criando tabela1 na curated

CREATE OR REPLACE TABLE `nifty-time-351417.curated.tabela1` AS 
(
  SELECT ANO, MES, SUM(QTD_VENDA) AS TOTAL_VENDAS
  FROM `nifty-time-351417.staging.tabela1`
  GROUP BY 1, 2
);

-- 2 criando tabela2 na curated

CREATE OR REPLACE TABLE `nifty-time-351417.curated.tabela2` AS 
(
  SELECT MARCA, LINHA, SUM(QTD_VENDA) AS TOTAL_VENDAS
  FROM `nifty-time-351417.staging.tabela2`
  GROUP BY 1, 2
);

-- 3 criando tabela3 na curated

CREATE OR REPLACE TABLE `nifty-time-351417.curated.tabela3` AS 
(
  SELECT MARCA, ANO, MES, SUM(QTD_VENDA) AS TOTAL_VENDAS
  FROM `nifty-time-351417.staging.tabela3`
  GROUP BY 1, 2, 3
);

-- 4 criando tabela4 na curated

CREATE OR REPLACE TABLE `nifty-time-351417.curated.tabela4` AS 
(
  SELECT LINHA, ANO, MES, SUM(QTD_VENDA) AS TOTAL_VENDAS
  FROM `nifty-time-351417.staging.tabela4`
  GROUP BY 1, 2, 3
);
