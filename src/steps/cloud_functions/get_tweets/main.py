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
    token = "token"
    
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
