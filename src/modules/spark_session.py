import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from loguru import logger

def get_spark_session(app_name: str) -> SparkSession:
    """
    Cria ou obtém uma SparkSession configurada com suporte nativo ao MinIO (S3A).
    Utiliza as credenciais carregadas a partir do arquivo .env.
    """
    logger.info("Iniciando a criação da SparkSession para a aplicação: '{}'", app_name)
    try:
        # Carrega as variáveis do arquivo .env
        load_dotenv()
        
        # Recupera as credenciais com fallback seguro
        minio_user = os.getenv("MINIO_ROOT_USER", "minioadmin")
        minio_password = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")

        logger.debug("Configurando Hadoop S3A com endpoint: '{}' e usuário: '{}'", minio_endpoint, minio_user)
        
        # Inicializa a builder da sessão do Spark
        spark = (
            SparkSession.builder
            .appName(app_name)
            # Baixa os pacotes Hadoop AWS e AWS Java SDK bundle para o Spark se comunicar com o S3/MinIO
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
            # Configurações do Hadoop FileSystem para MinIO
            .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", minio_user)
            .config("spark.hadoop.fs.s3a.secret.key", minio_password)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )
        
        logger.info("SparkSession '{}' criada e configurada com sucesso.", app_name)
        return spark
        
    except Exception as e:
        logger.error("Falha ao inicializar a SparkSession: {}", str(e))
        raise e

def close_spark_session(spark: SparkSession) -> None:
    """
    Encerra com segurança a sessão ativa do Spark.
    """
    if spark:
        logger.info("Encerrando a SparkSession ativa.")
        spark.stop()
        logger.info("SparkSession finalizada com sucesso.")
    else:
        logger.warning("Tentativa de fechar SparkSession nula ou inexistente.")
