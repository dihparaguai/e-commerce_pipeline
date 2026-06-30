import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Adiciona o diretório base (/opt/airflow) ao sys.path para reconhecer o módulo 'src'
sys.path.append(str(Path(__file__).resolve().parent.parent))
from src.modules.minio_utils import create_minio_bucket

def create_and_show_spark_df(): 
    """
    Inicializa ou obtém uma sessão ativa do Spark com suporte ao S3A/MinIO.
    """
    load_dotenv()

    MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
    MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        
    # O appName define o nome da aplicação que aparecerá no Spark Web UI.
    spark = (
        SparkSession.builder
        .appName("TestSparkApp")
        # Baixa dinamicamente as dependências necessárias para suporte ao S3/MinIO
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        # Configurações do Hadoop para conectar no endpoint do MinIO local
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ROOT_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_ROOT_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    
    # Cria o bucket 'test-spark' antes de tentar gravar nele
    create_minio_bucket("test-spark")
    
    data = [("diego", 28), ("rodrigo", 27)]
    columns = ["nome", "idade"]
    df = spark.createDataFrame(data, columns)
    df.show()
    
    # Salva o DataFrame no bucket 'test-spark' do MinIO como Parquet particionado pela coluna 'nome'
    df.write.mode("overwrite").partitionBy("nome").parquet("s3a://test-spark/parquet_data")
    
    # Finaliza a sessão do Spark
    spark.stop()

if __name__ == "__main__":
    create_and_show_spark_df()