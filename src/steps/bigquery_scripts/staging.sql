  -- 1 criando a tabela 1 na staging

CREATE OR REPLACE TABLE
  `nifty-time-351417.staging.tabela1` AS
SELECT
  EXTRACT(YEAR
  FROM
    DATA_VENDA) AS ANO,
  EXTRACT(MONTH
  FROM
    DATA_VENDA) AS MES,
  QTD_VENDA
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY DATA_VENDA, QTD_VENDA, ID_LINHA, ID_MARCA) AS rnk
  FROM
    `nifty-time-351417.raw.base*`) A
WHERE
  rnk = 1;


  -- 2 criando a tabela 2 na staging

CREATE OR REPLACE TABLE
  `nifty-time-351417.staging.tabela2` AS
SELECT
  MARCA,
  LINHA,
  QTD_VENDA
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY DATA_VENDA, QTD_VENDA, ID_LINHA, ID_MARCA) AS rnk
  FROM
    `nifty-time-351417.raw.base*`) A
WHERE
  rnk = 1;


  -- 3 criando a tabela 3 na staging

CREATE OR REPLACE TABLE
  `nifty-time-351417.staging.tabela3` AS
SELECT
  MARCA,
  EXTRACT(YEAR
  FROM
    DATA_VENDA) AS ANO,
  EXTRACT(MONTH
  FROM
    DATA_VENDA) AS MES,
  QTD_VENDA
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY DATA_VENDA, QTD_VENDA, ID_LINHA, ID_MARCA) AS rnk
  FROM
    `nifty-time-351417.raw.base*`) A
WHERE
  rnk = 1;


-- 4 criando a tabela 4 na staging

CREATE OR REPLACE TABLE
  `nifty-time-351417.staging.tabela4` AS
SELECT
  LINHA,
  EXTRACT(YEAR
  FROM
    DATA_VENDA) AS ANO,
  EXTRACT(MONTH
  FROM
    DATA_VENDA) AS MES,
  QTD_VENDA
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY DATA_VENDA, QTD_VENDA, ID_LINHA, ID_MARCA) AS rnk
  FROM
    `nifty-time-351417.raw.base*`) A
WHERE
  rnk = 1;
