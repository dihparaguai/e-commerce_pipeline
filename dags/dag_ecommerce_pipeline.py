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
JOBS_BASE_PATH = "/opt/airflow/src/jobs"

# Define a função sem parametros, pra não precisar usar o kwargs no PythonOperator
def create_buckets_task():
    """
    Garante que o bucket bronze, silver e gold esteja criado no MinIO.
    """
    create_minio_bucket("bronze")
    create_minio_bucket("silver")
    create_minio_bucket("gold")

# Configuração dos argumentos padrão da DAG do pipeline
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
    dag_id="dag_ecommerce_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Execução manual
    catchup=False,
    tags=['ecommerce', 'pipeline', 'minio', 'postgres'],
) as dag:

    # Setup dos Buckets
    create_buckets = PythonOperator(
        task_id="create_buckets_task",
        python_callable=create_buckets_task,
    )

    # Ingestão de Vendas
    ingest_vendas = SparkSubmitOperator(
        task_id="ingest_vendas_to_bronze",
        application=f"{JOBS_BASE_PATH}/ingest/ingest_vendas.py",
        conn_id=SPARK_CONN_ID,
        verbose=True
    )

    # Ingestão de Estoque
    ingest_estoque = SparkSubmitOperator(
        task_id="ingest_estoque_to_bronze",
        application=f"{JOBS_BASE_PATH}/ingest/ingest_estoque.py",
        conn_id=SPARK_CONN_ID,
        verbose=True
    )

    # Ingestão de Devoluções
    ingest_devolucoes = SparkSubmitOperator(
        task_id="ingest_devolucoes_to_bronze",
        application=f"{JOBS_BASE_PATH}/ingest/ingest_devolucoes.py",
        conn_id=SPARK_CONN_ID,
        verbose=True
    )

    # Transformação de Vendas
    transform_vendas = SparkSubmitOperator(
        task_id="transform_vendas_to_silver",
        application=f"{JOBS_BASE_PATH}/transform/transform_vendas.py",
        conn_id=SPARK_CONN_ID,
        verbose=True
    )

    # Transformação de Devoluções
    transform_devolucoes = SparkSubmitOperator(
        task_id="transform_devolucoes_to_silver",
        application=f"{JOBS_BASE_PATH}/transform/transform_devolucoes.py",
        conn_id=SPARK_CONN_ID,
        verbose=True
    )

    # Transformação de Estoque
    transform_estoque = SparkSubmitOperator(
        task_id="transform_estoque_to_silver",
        application=f"{JOBS_BASE_PATH}/transform/transform_estoque.py",
        conn_id=SPARK_CONN_ID,
        verbose=True
    )

    # Modelagem de Dimensão Produto
    modeling_dim_produto = SparkSubmitOperator(
        task_id="modeling_dim_produto_to_gold",
        application=f"{JOBS_BASE_PATH}/modeling/modeling_dim_produto.py",
        conn_id=SPARK_CONN_ID,
        verbose=True
    )

    # Definição do fluxo do pipeline:
    create_buckets >> [ingest_vendas, ingest_estoque, ingest_devolucoes]
    ingest_vendas >> transform_vendas
    ingest_devolucoes >> transform_devolucoes
    ingest_estoque >> transform_estoque
    [transform_vendas, transform_devolucoes, transform_estoque] >> modeling_dim_produto
