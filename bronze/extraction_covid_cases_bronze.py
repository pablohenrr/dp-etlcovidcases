import requests
import json
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

load_dotenv()

CONNECTION_STRING = os.getenv('CONNECTION_STRING')
CONTAINER_NAME = os.getenv('CONTAINER_NAME')
BLOB_FOLDER = os.getenv('BLOB_FOLDER')
BLOB_FILE_NAME = os.getenv('BLOB_FILE_NAME')

url = 'https://covid19-brazil-api.now.sh/api/report/v1'

response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    json_data = json.dumps(data)

    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)

    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    if not container_client.exists():
        container_client.create_container()

    blob_client = container_client.get_blob_client(f'{BLOB_FOLDER}/{BLOB_FILE_NAME}')
    blob_client.upload_blob(json_data, overwrite=True)

    print('Dados enviados para o Azure Data Lake com sucesso!')
else:
    print(f'Erro ao acessar a API: {response.status_code}')