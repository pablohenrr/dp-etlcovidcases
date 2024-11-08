import json
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
from datetime import datetime
import logging
from io import BytesIO

logging.basicConfig(level=logging.INFO)

load_dotenv()

CONNECTION_STRING = os.getenv('CONNECTION_STRING')
CONTAINER_NAME = os.getenv('CONTAINER_NAME')
BLOB_FOLDER_BRONZE = 'covid-bronze'
BLOB_FOLDER_SILVER = 'covid-silver'
BLOB_FILE_NAME = os.getenv('BLOB_FILE_NAME')

BLOB_NAME_BRONZE = f'{BLOB_FOLDER_BRONZE}/{BLOB_FILE_NAME}'
BLOB_NAME_SILVER_FACT = f'{BLOB_FOLDER_SILVER}/fato_covid.parquet'
BLOB_NAME_SILVER_DIM_DATA = f'{BLOB_FOLDER_SILVER}/dim_data.parquet'
BLOB_NAME_SILVER_DIM_ESTADO = f'{BLOB_FOLDER_SILVER}/dim_estado.parquet'

blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

logging.info("Baixando dados da camada bronze.")
blob_client = container_client.get_blob_client(BLOB_NAME_BRONZE)
download_stream = blob_client.download_blob()
json_data = json.loads(download_stream.readall())

df = pd.json_normalize(json_data['data'])

logging.info("Realizando limpeza e transformação dos dados.")

df['data'] = pd.to_datetime(df['datetime']).dt.date
df.fillna({'cases': 0, 'deaths': 0, 'suspects': 0, 'refuses': 0}, inplace=True)

dim_data = df[['data']].drop_duplicates().reset_index(drop=True)
dim_data['data'] = pd.to_datetime(dim_data['data'])
dim_data['id_data'] = dim_data['data'].apply(lambda x: int(x.strftime('%Y%m%d')))
dim_data['ano'] = dim_data['data'].dt.year
dim_data['mes'] = dim_data['data'].dt.month
dim_data['dia'] = dim_data['data'].dt.day
dim_data['dia_semana'] = dim_data['data'].dt.dayofweek + 1  
dim_data['trimestre'] = dim_data['data'].dt.quarter
dim_data['semestre'] = dim_data['data'].dt.month.apply(lambda x: 1 if x <= 6 else 2)

dim_estado = df[['uid', 'uf', 'state']].drop_duplicates().reset_index(drop=True)
dim_estado.rename(columns={'uid': 'id_estado', 'uf': 'sigla_estado', 'state': 'nome_estado'}, inplace=True)
regioes = {
    'AC': 'Norte', 'AL': 'Nordeste', 'AP': 'Norte', 'AM': 'Norte', 'BA': 'Nordeste', 
    'CE': 'Nordeste', 'DF': 'Centro-Oeste', 'ES': 'Sudeste', 'GO': 'Centro-Oeste', 
    'MA': 'Nordeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste', 'MG': 'Sudeste', 
    'PA': 'Norte', 'PB': 'Nordeste', 'PR': 'Sul', 'PE': 'Nordeste', 'PI': 'Nordeste', 
    'RJ': 'Sudeste', 'RN': 'Nordeste', 'RS': 'Sul', 'RO': 'Norte', 'RR': 'Norte', 
    'SC': 'Sul', 'SP': 'Sudeste', 'SE': 'Nordeste', 'TO': 'Norte'
}
dim_estado['regiao'] = dim_estado['sigla_estado'].map(regioes)

df['data'] = pd.to_datetime(df['data'])

fato_covid = df.merge(dim_data, on='data')
fato_covid = fato_covid.merge(dim_estado, left_on='uid', right_on='id_estado')
fato_covid = fato_covid[['id_data', 'id_estado', 'cases', 'deaths', 'suspects', 'refuses']]
fato_covid.rename(columns={
    'cases': 'casos_confirmados',
    'deaths': 'mortes',
    'suspects': 'casos_suspeitos',
    'refuses': 'casos_descartados'
}, inplace=True)

fato_covid = fato_covid.astype({
    'casos_confirmados': 'int64', 
    'mortes': 'int64', 
    'casos_suspeitos': 'int64', 
    'casos_descartados': 'int64'
})

def save_df_to_blob(df, blob_name, append=False):
    output = BytesIO()
    blob_client = container_client.get_blob_client(blob_name)

    if append and blob_client.exists():
        logging.info(f"Arquivo {blob_name} existe. Realizando append aos dados.")
        existing_df = pd.read_parquet(BytesIO(blob_client.download_blob().readall()))
        df = pd.concat([existing_df, df]).drop_duplicates().reset_index(drop=True)

    df.to_parquet(output, index=False)
    output.seek(0)
    blob_client.upload_blob(output, overwrite=True)
    logging.info(f"Dados salvos no blob: {blob_name}")

save_df_to_blob(dim_data, BLOB_NAME_SILVER_DIM_DATA, append=True)
save_df_to_blob(dim_estado, BLOB_NAME_SILVER_DIM_ESTADO, append=True)
save_df_to_blob(fato_covid, BLOB_NAME_SILVER_FACT, append=True)

logging.info("Dados transformados e salvos na camada prata com sucesso!")