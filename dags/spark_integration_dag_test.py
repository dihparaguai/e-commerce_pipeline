from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Configuração dos argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'retries': 0, # Número de tentativas em caso de erro
    'retry_delay': timedelta(seconds=15), # Tempo de espera entre as tentativas
}

# Definição da DAG para orquestração do pipeline
with DAG(
    dag_id="spark_integration_dag_test",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None, # Execução manual (sem agendamento automático)
    catchup=False, # Evita execuções retroativas automáticas
    tags=['test'],
) as dag:

    # Task para enviar o job PySpark ao Spark Master
    submit_job = SparkSubmitOperator(
        task_id="submit_spark_job",
        application="/opt/airflow/tests/test_spark.py", # Caminho do script dentro do container do Airflow/Spark
        conn_id="spark_default", # ID da conexão configurada no UI do Airflow para apontar para o Spark Master
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262", # Dependências para conexão com o MinIO (S3A)
        verbose=True
    )