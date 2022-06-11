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
