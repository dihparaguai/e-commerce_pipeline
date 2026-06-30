import os
import sys

# Adiciona o diretório base (/opt/airflow) ao sys.path para reconhecer o módulo 'src'
sys.path.append("/opt/airflow")

from src.modules.spark_session import get_spark_session, close_spark_session
from loguru import logger

def ingest_devolucoes() -> None:
    """
    Job PySpark para ler a base de devoluções brutos (CSV) e gravá-la na camada Bronze no MinIO
    como Parquet particionado pelas colunas: data_devolucao e status_devolucao.
    """
    logger.info("Iniciando a ingestão de devoluções (Raw -> Bronze)...")
    
    # Inicializa a sessão do Spark
    spark = get_spark_session("IngestionDevolucoesBronze")
    
    try:
        # Define os caminhos de origem e destino
        raw_csv_path = "/opt/airflow/data/raw/devolucoes.csv"
        bronze_parquet_path = "s3a://bronze/devolucoes"
        
        logger.info("Lendo arquivo CSV de origem: '{}'", raw_csv_path)
        
        # Lê o CSV inferindo o schema e assumindo a presença de cabeçalho
        df = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(raw_csv_path)
        )
        
        # Lógica de Ingestão Incremental: remover duplicados da própria carga nova e filtrar IDs que já existem no histórico
        try:
            logger.info("Buscando dados existentes no destino para realizar o filtro incremental...")
            df_existing = spark.read.parquet(bronze_parquet_path)
            
            # Remove duplicados internos da nova carga com base na chave primária
            df_new_unique = df.dropDuplicates(["devolucao_id"])
            
            # Mantém apenas os registros novos que não existem no histórico
            df_to_append = df_new_unique.join(df_existing, on="devolucao_id", how="left_anti")
            logger.info("Dados existentes encontrados. Filtrando chaves duplicadas.")
        except Exception:
            logger.info("Nenhum dado existente encontrado. Preparando primeira carga.")
            df_to_append = df.dropDuplicates(["devolucao_id"])

        # Log da quantidade de registros a serem inseridos para validação
        logger.info("Total de registros a serem inseridos: {}", df_to_append.count())

        # Grava os dados particionados na Bronze
        logger.info("Gravando dados no formato Parquet no MinIO (modo: append) em: '{}'", bronze_parquet_path)
        (
            df_to_append.write
            .mode("append")
            .partitionBy("data_devolucao", "status_devolucao")
            .parquet(bronze_parquet_path)
        )
        
        logger.info("Ingestão de devoluções finalizada com sucesso!")
        
    except Exception as e:
        logger.error("Falha durante o processamento de ingestão de devoluções: {}", str(e))
        raise e
        
    finally:
        # Garante o encerramento da sessão
        close_spark_session(spark)

if __name__ == "__main__":
    ingest_devolucoes()
