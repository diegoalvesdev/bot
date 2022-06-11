# Pipeline Bot
Proposta de implementação do projeto com criação de Pipeline na Google Cloud Plataform, solução em nuvem oferecida pelo Google.

## Pipeline

<!-- TOC start -->
- [Arquivos XLSX](#arquivos-xlsx)
- [Staging e curated area](#staging-e-curated-area)
- [Twitter API](#twitter-api)
<!-- TOC end -->

### Arquivos XLSX

Para o desenvolvimento do step que diz respeito aos arquivos XLSX, a proposta compoe utilizacao de uma Cloud Function associada a trigger. O evento que dispara a trigger é a criacao de objetos em bucket do Google Storage. 

~~~python

import pandas as pd
import pandas_gbq
from google.cloud import bigquery


def import_xlsx(event, context):
    """Triggered by a change to a Cloud Storage bucket. 
    Function to access file uploaded to the bucket and transform it into a table in Bq.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    
    file = event
    nome_bucket = file['bucket']
    nome_objeto = file['name']
     
    # Instanciando cliente do BQ
    client = bigquery.Client()

    # Caminho para o objeto do bucket
    caminho_bucket = 'gs://' + nome_bucket + '/' + nome_objeto

    # Lendo objeto do bucket e armazenando como dataframe
    df = pd.read_excel(caminho_bucket, index_col=False)

    # Replace do . da extensao do arquivo para traco
    nome_tabela_bq = "raw." + nome_objeto.replace('.','-')

    # Identificador da tabela no BQ
    identificador_tabela = "nifty-time-351417.raw" + nome_tabela_bq

    # Se a tabela nao existir cria ela e insere o dataframe na tabela nova
    try:
        client.get_table(identificador_tabela)
    except:
        pandas_gbq.to_gbq(df, nome_tabela_bq)

~~~

### Staging e curated area

Para o desenvolvimento do step que diz respeito aos arquivos XLSX, a proposta compoe utilizacao de uma Cloud Function associada a trigger. O evento que dispara a trigger é a criacao de objetos em bucket do Google Storage. 


#### Staging area

~~~sql

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
~~~

#### Curated area


~~~sql
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
~~~


### Twitter API

Para o desenvolvimento do step que diz respeito aos arquivos XLSX, a proposta compoe utilizacao de uma Cloud Function associada a trigger. O evento que dispara a trigger é a criacao de objetos em bucket do Google Storage. 

~~~python

import base64
import tweepy
import pandas as pd
from google.cloud import bigquery
import pandas_gbq


def get_tweets(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic. 
    Function to consult curated table, search for tweets with the best selling line and save results in BQ.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    
    # Mensagem do pubsub
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    
    # Variaveis de mes e ano para consulta no BQ
    mes = '12'
    ano = '2019'
    
    # Caminho para tabela curated no BQ
    tabela_consulta = "nifty-time-351417.curated.tabela4"
    
    # Caminho para tabela que sera salva com tuites no dataset curated
    tabela_tweets = "curated.tweets-"+ano+mes
    
    # Token para uso da API do Twitter
    token = "AAAAAAAAAAAAAAAAAAAAAPxWZQEAAAAA375fwznpGjKBqJ%2FFD%2BufaVbGBOo%3DQA3XWwzIAZOapm1ATAARWBRUemMVIQ4gO3hlZGw7msHZyKHFg6"
    
    # Instanciando cliente do BQ
    client = bigquery.Client()
    
    # Setando configuracoes do job
    job_config = bigquery.job.QueryJobConfig(use_query_cache=True)
    
    # Query para buscar linha mais vendida no ano e mes escolhidos
    bq_query = f"""
                SELECT LINHA, MAX(TOTAL_VENDAS) AS VENDAS
                FROM `{tabela_consulta}`
                    WHERE MES = {mes} AND ANO = {ano}
                    GROUP BY LINHA 
                    ORDER BY VENDAS DESC 
                    LIMIT 1
    """
    
    # Executando a query
    job_name = client.query(bq_query, job_config = job_config)
    
    # Retornando resultados como dataframe
    df_bq = job_name.result().to_dataframe()

    # Filtrando apenas linha mais vendida em string
    linha_mais_vendas = df_bq['LINHA'].to_string(index=False)

    # Montando query de consulta a API do Twitter
    search_query = f"({linha_mais_vendas} OR Boticário) -is:retweet lang:pt"

    # Instanciando o objeto para usar a API
    client = tweepy.Client(bearer_token=token)

    # Executando a query, buscando os ultimos 50 resultados
    tweets = client.search_recent_tweets(query = search_query, max_results = 50, user_fields = ["username"], expansions='author_id')

    # Lista para tuites com mensagem e nome do usuario
    tweet_info_ls = []
    
    # Percorrendo as mensagens encontradas
    for tweet, user in zip(tweets.data, tweets.includes['users']):
        tweet_info = {
            'MENSAGEM': tweet.text,
            'USUARIO': user.username,
        }
        tweet_info_ls.append(tweet_info)
        
    # Transformando a lista de tuites em dataframe
    df_tweets = pd.DataFrame(tweet_info_ls)
    
    # Adicionando campo da linha mais vendida ao dataframe
    df_tweets['LINHA_MAIS_VENDIDA'] = linha_mais_vendas
    
    # Salvando tabela de tuites, se ja existir faz append dos resultados
    pandas_gbq.to_gbq(df_tweets, tabela_tweets, if_exists='append')
    
    # Imprimindo mensagem do pub/sub
    print(pubsub_message)
~~~
        
