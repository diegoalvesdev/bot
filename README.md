# Pipeline Bot
Proposta de implementação do projeto com criação de Pipeline na Google Cloud Plataform, solução em nuvem oferecida pelo Google. Para execucao do projeto, foram utilizadas solucoes GCP como: Coogle Storage, Cloud Scheduler, BigQuery, Cloud Pub/Sub e Cloud Function.

![image](image.png)

## Pipeline

<!-- TOC start -->
- [Arquivos XLSX](#arquivos-xlsx)
- [Staging e curated](#staging-e-curated)
- [Twitter API](#twitter-api)
<!-- TOC end -->

### Arquivos XLSX

Para o desenvolvimento do step que diz respeito aos arquivos XLSX, a proposta compoe utilizacao de uma Cloud Function associada a trigger. O evento que dispara a trigger é a criacao de objetos em bucket do Google Storage. Em resumo, quando é adicionado o arquivo XLSX, a function pega o nome do arquivo e cria uma tabela na raw do BigQuery com o mesmo nome do arquivo. Para tal tarefa, utilizou-se biblioteca pandas_gbq. Ela faz o mapeamento dos campos do objeto XLS para campos na tabela do BQ, de forma automatica. Muito semelhante a ideia do ORM.

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

### Staging e curated


#### Staging area

Pensando em processos de Engenharia de Dados, temos *staging area* como local temporário onde os dados oriundos de arquivos XLSX (já convertidos para o BigQuery na Raw) são copiados. Desta forma, ao invés de acessar os dados diretamente da fonte (Raw), o processo de “transformação” do ETL pega os dados da staging.

Para montagem da staging, algumas queries são executadas através de *Schedule* no BigQuery. Os scripts fazem o trabalho de selecao dos campos indicados pelas regras de negocio, bem como tratamento de dados que nao sao passiveis de analise, como dados duplicados. O agendamento ocorre para que inicialmente a staging seja montada e posteriormente a curated fique disponivel. Sempre pensando na possibilidade de nao existir limitacao quanto a quantidade de tabelas (a partir de objetos XLSX) disponiveis na raw.

~~~sql

  -- Exemplo de criacao da tabela 1 na staging

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

~~~

#### Curated

Pensando na camada curated, temos dados transformados e agredados para atender os requisitos do projeto. Temos nela, a uniao de dados brutos e prontos para serem consumidos por times de analistas e/ou cientistas de dados.

O grau de documentacao desta zona é importante, visto que ela deve ser alinhada a regras de negócio. No caso, abaixo temos a criacao da tabela 1, atendendo a regra de ter total de vendas para cada conjunto de mes / ano. Os dados, para montagem desta camada vem da staging area.

~~~sql

-- Exemplo de criacao da tabela 1 na curated, alinhada aos requisitos

CREATE OR REPLACE TABLE `nifty-time-351417.curated.tabela1` AS 
(
  SELECT ANO, MES, SUM(QTD_VENDA) AS TOTAL_VENDAS
  FROM `nifty-time-351417.staging.tabela1`
  GROUP BY 1, 2
);

~~~


### Twitter API

Como forma de enriquecer os dados e gerar valor ao negócio. Foi proposta a utilizacao da API do Twitter com o objetivo de, apresentar os 50 mais recentes tuites e usuarios que fizeram referencia ao produto mais vendido em dezembro de 2019, bem como a marca Boticário.

Para tal, foi utilizada uma Cloud Function, disparada por tópico criado no Cloud Pub/Sub. Este topico é agendada para ser o ultimo step do nosso pipeline. Ele ocorre, posteriormente a consolidacao da staging area, bem como a nossa cama de curated. É através da camada curated que conseguimos extrair a linha mais vendida em dezembro de 2019 (curated.tabela4 no BigQuery).

O script abaixo busca a linha mais vendida, utilizando ela como parametro para encontrar os tuítes. Ao final, ela consolida os resultados em uma nova tabela no BigQuery. Criando a tabela na primeira vez que a function é executada e posteriormente dando append nos dados. Finalizando assim o pipeline.


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
        
