import os
from minio import Minio
from dotenv import load_dotenv
from loguru import logger

def create_minio_bucket(bucket_name: str) -> None:
    """
    Cria um bucket no MinIO usando o SDK oficial em Python, caso ele não exista.
    """
    logger.info("Iniciando criação de bucket no MinIO: {}", bucket_name)
    try:
        # Carrega variáveis do .env
        load_dotenv()

        # Obtém credenciais do MinIO
        MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
        MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

        # Cria o client do MinIO para conexão
        client = Minio(
        endpoint="minio:9000",
            access_key=MINIO_ROOT_USER,
            secret_key=MINIO_ROOT_PASSWORD,
            secure=False
        )
        
        # Cria o bucket se não existir
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info("Bucket '{}' criado com sucesso no MinIO.", bucket_name)
        else:
            logger.info("Bucket '{}' já existe no MinIO. Nenhuma ação necessária.", bucket_name)
            
    except Exception as e:
        logger.error("Falha ao verificar/criar bucket '{}' no MinIO. Detalhes: {}", bucket_name, str(e))
        raise e
