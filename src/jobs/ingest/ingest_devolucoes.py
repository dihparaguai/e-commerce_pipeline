import os
import sys
from loguru import logger
from datetime import date

# Adiciona o diretório base (/opt/airflow) ao sys.path para reconhecer o módulo 'src'
sys.path.append("/opt/airflow")
from src.services.spark_session import get_spark_session, close_spark_session
import src.modules.ingest_utils as ingest
import src.modules.utils as utils

def ingest_devolucoes() -> None:
    """
    Job PySpark para ler novos arquivos de devoluções (CSV) e gravá-los na camada Bronze no MinIO com controle de CDC local (evitando duplicar arquivos já processados) e de chaves.
    """
    logger.info("Iniciando a ingestão de devoluções (Raw -> Bronze)...")
    raw_dir = "/opt/airflow/data/raw/devolucoes"
    log_dir = "/opt/airflow/data/cdc"
    log_filename = "devolucoes.csv"
    bronze_parquet_path = "s3a://bronze/devolucoes"
    
    # Obtém a lista de arquivos novos
    new_files = ingest.get_new_files(raw_dir, log_dir, log_filename)
    
    if not new_files:
        logger.info("Nenhum arquivo novo de devoluções encontrado para ingestão.")
        return
    logger.info("Arquivos novos de devoluções detectados para processamento: {}", new_files)
    
    # Inicializa a sessão do Spark
    spark = get_spark_session("IngestionDevolucoesBronze")
    try:
        data_carga_str = utils.get_current_date_str()
        
        # Lê os CSVs novos e adiciona data_carga
        df = ingest.read_new_csv_files(spark, raw_dir, new_files, data_carga_str)
        
        # Filtra os dados duplicados locais e existentes no histórico da Bronze
        df_to_append = ingest.deduplicate_and_filter_existing(spark, df, "devolucao_id", bronze_parquet_path)
        
        logger.info("Total de registros a serem inseridos: {}", df_to_append.count())

        # Grava os dados particionados na Bronze
        logger.info("Gravando dados no formato Parquet no MinIO (modo: append) em: '{}'", bronze_parquet_path)
        (
            df_to_append.write
            .mode("append")
            .partitionBy("data_carga")
            .parquet(bronze_parquet_path)
        )
        
        # Registra os arquivos como processados no log de CDC após o sucesso da gravação
        ingest.register_processed_files(log_dir, log_filename, new_files, data_carga_str)
        logger.info("Ingestão de devoluções finalizada com sucesso!")
        
    except Exception as e:
        logger.error("Falha durante o processamento de ingestão de devoluções: {}", str(e))
        raise e
        
    finally:
        # Garante o encerramento da sessão
        close_spark_session(spark)

if __name__ == "__main__":
    ingest_devolucoes()
