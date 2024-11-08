import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
import logging
from io import BytesIO

logging.basicConfig(level=logging.INFO)

load_dotenv()

CONNECTION_STRING = os.getenv('CONNECTION_STRING')
CONTAINER_NAME = os.getenv('CONTAINER_NAME')
BLOB_FOLDER_SILVER = 'covid-silver'
BLOB_FOLDER_GOLD = 'covid-gold'
BLOB_NAME_GOLD_VIEW = f'{BLOB_FOLDER_GOLD}/covid_cases_view.parquet'

BLOB_NAME_SILVER_FACT = f'{BLOB_FOLDER_SILVER}/fato_covid.parquet'
BLOB_NAME_SILVER_DIM_DATA = f'{BLOB_FOLDER_SILVER}/dim_data.parquet'
BLOB_NAME_SILVER_DIM_ESTADO = f'{BLOB_FOLDER_SILVER}/dim_estado.parquet'

blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

def read_parquet_from_blob(blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    download_stream = blob_client.download_blob()
    return pd.read_parquet(BytesIO(download_stream.readall()))

logging.info("Lendo tabelas da camada prata.")
dim_data = read_parquet_from_blob(BLOB_NAME_SILVER_DIM_DATA)
dim_estado = read_parquet_from_blob(BLOB_NAME_SILVER_DIM_ESTADO)
fato_covid = read_parquet_from_blob(BLOB_NAME_SILVER_FACT)

logging.info("Criando a view unificada para a camada ouro.")
unified_view = fato_covid.merge(dim_data, on='id_data').merge(dim_estado, on='id_estado')

def save_df_to_blob(df, blob_name):
    output = BytesIO()
    df.to_parquet(output, index=False)
    output.seek(0)
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(output, overwrite=True)
    logging.info(f"View unificada salva no blob: {blob_name}")

save_df_to_blob(unified_view, BLOB_NAME_GOLD_VIEW)

logging.info("View unificada criada e salva na camada ouro com sucesso!")