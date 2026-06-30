import sys
from pathlib import Path

# Adiciona o diretório base (/opt/airflow) ao sys.path para reconhecer o módulo 'src'
sys.path.append(str(Path(__file__).resolve().parent.parent))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from loguru import logger
from src.modules.minio_utils import create_minio_bucket

# Configurações globais centralizadas do Spark
SPARK_CONN_ID = "spark_default"
SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
JOBS_BASE_PATH = "/opt/airflow/src/jobs/ingest"

# Define a função sem parametros, pra não precisar usar o kwargs no PythonOperator
def create_bronze_bucket_task():
    """
    Garante que o bucket bronze esteja criado no MinIO.
    """
    create_minio_bucket("bronze")

# Configuração dos argumentos padrão da DAG do pipeline
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
    dag_id="dag_ecommerce_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Execução manual
    catchup=False,
    tags=['ecommerce', 'bronze', 'pipeline', 'minio'],
) as dag:

    # Setup do Bucket Bronze
    create_bronze_bucket = PythonOperator(
        task_id="create_bronze_bucket",
        python_callable=create_bronze_bucket_task,
    )

    # Ingestão de Vendas (Spark)
    ingest_vendas = SparkSubmitOperator(
        task_id="ingest_vendas_to_bronze",
        application=f"{JOBS_BASE_PATH}/ingest_vendas.py",
        conn_id=SPARK_CONN_ID,
        packages=SPARK_PACKAGES,
        conf={"spark.jars.ivy": "/tmp/.ivy-vendas"},
        verbose=True
    )

    # Ingestão de Estoque (Spark)
    ingest_estoque = SparkSubmitOperator(
        task_id="ingest_estoque_to_bronze",
        application=f"{JOBS_BASE_PATH}/ingest_estoque.py",
        conn_id=SPARK_CONN_ID,
        packages=SPARK_PACKAGES,
        conf={"spark.jars.ivy": "/tmp/.ivy-estoque"},
        verbose=True
    )

    # Ingestão de Devoluções (Spark)
    ingest_devolucoes = SparkSubmitOperator(
        task_id="ingest_devolucoes_to_bronze",
        application=f"{JOBS_BASE_PATH}/ingest_devolucoes.py",
        conn_id=SPARK_CONN_ID,
        packages=SPARK_PACKAGES,
        conf={"spark.jars.ivy": "/tmp/.ivy-devolucoes"},
        verbose=True
    )

    # Definição do fluxo do pipeline:
    # 1. Cria Bucket Bronze -> 2. Dispara as 3 ingestões em paralelo
    create_bronze_bucket >> [ingest_vendas, ingest_estoque, ingest_devolucoes]
