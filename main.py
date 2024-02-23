import os
import boto3
import json
import logging

ACCESS_KEY=""
SECRET_KEY=""
BUCKET_NAME=""
REGION=""


def login_aws_cli_credenciais():
    # Obtém as credenciais da AWS a partir de variáveis de ambiente
    aws_access_key_id = ACCESS_KEY
    aws_secret_access_key = SECRET_KEY
    aws_region = REGION
    return aws_access_key_id, aws_secret_access_key, aws_region

def download_json_bucket(bucket_name, json_key):
    """
        Função realizara o download do principal arquivo de json. Com ele irá existir todos os arns
        que precisamos para realizar os mocks, será enviado por cada área do banco, com suas respectivas
        configurações.
    """
    aws_access_key_id, aws_secret_access_key, aws_region = login_aws_cli_credenciais()
    s3 = boto3.client('s3', 
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region)
    try:
        s3.download_file(bucket_name, json_key, json_key)
        return True
    except Exception as e:
        logging.error(f"Falha ao realizaro download do JSON bucket: {bucket_name}: {e}")
        return False

def download_arquivos_buckets_mocks(bucket_name):
    """
        Função realizara o download dos arquivos de mocks, todas configurações por produto será 
        possível a configuração local e quando subir para os ambientes docker e homologação será 
        indexado todos os mocks.
    """
    aws_access_key_id, aws_secret_access_key, aws_region = login_aws_cli_credenciais()
    s3 = boto3.client('s3', 
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region)

    objects = s3.list_objects_v2(Bucket=bucket_name)
    if 'Contents' in objects:
        for obj in objects['Contents']:
            key = obj['Key']
            os.makedirs(os.path.join(bucket_name, key), exist_ok=True)
            s3.download_file(bucket_name, key, os.path.join(bucket_name, key))

    logging.info(f"Arquivos do bucket {bucket_name} carregados com sucesso!")

def main():
    with open('arns.json', 'r') as file:
        arns = json.load(file)

        # Itera sobre cada ARN e baixa os arquivos dos respectivos buckets
        for arn in arns['arns']:
            bucket_product_name = arn['produto']
            bucket_name = arn['bucket']
            logging.info(f"Downloading arquivos produto [{bucket_product_name}] bucket:{bucket_name}...")
            os.makedirs(bucket_name, exist_ok=True)
            download_arquivos_buckets_mocks(bucket_name)

        logging.info("Todos arquivos mocks baixados com sucesso.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
