import sys

# Adiciona o diretório base (/opt/airflow) ao sys.path para reconhecer o módulo 'src'
sys.path.append("/opt/airflow")

from loguru import logger
from pyspark.sql import functions as F

from src.modules.spark_session import get_spark_session, close_spark_session
import src.modules.transform_utils as transform

def run_transform_devolucoes() -> None:
    """
    Job PySpark para ler a base de devoluções na camada Bronze, aplicar filtros incrementais, transformações genéricas, e gravar o resultado na camada Silver no MinIO.
    """
    logger.info("Iniciando o job de transformação de devoluções (Bronze -> Silver)...")
    
    # Inicializa a sessão do Spark
    spark = get_spark_session("TransformDevolucoesSilver")
    
    # Define os caminhos de origem e destino no MinIO
    bronze_path = "s3a://bronze/devolucoes"
    silver_path = "s3a://silver/devolucoes"
    
    try:
        # Lê os dados da camada Bronze e renomeia data_carga para data_carga_bronze
        logger.info("Lendo dados da camada Bronze de: '{}'", bronze_path)
        df = spark.read.parquet(bronze_path).withColumnRenamed("data_carga", "data_carga_bronze")
        
        # Filtros de Carga Incremental
        try:
            logger.info("Lendo dados existentes na camada Silver para validação incremental...")
            df_silver = spark.read.parquet(silver_path)
            
            # Filtro pela data_carga_bronze máxima presente na Silver
            max_date_row = df_silver.select(F.max("data_carga_bronze")).collect()[0]
            max_data_carga = max_date_row[0]
            
            if max_data_carga:
                logger.info("Data máxima de carga encontrada na Silver: {}. Filtrando dados novos da Bronze...", max_data_carga)
                df = df.filter(F.col("data_carga_bronze") > max_data_carga)
                
            # Left anti join com a chave primária devolucao_id
            df = df.join(df_silver, on="devolucao_id", how="left_anti")
            logger.info("Filtros incrementais aplicados.")
            
        except Exception as e:
            logger.info("Camada Silver vazia ou não encontrada. Executando carga completa inicial.")
            
        # Total de registros a processar após filtros
        total_records = df.count()
        logger.info("Registros a serem processados nesta carga incremental: {}", total_records)
        
        if total_records == 0:
            logger.info("Nenhum registro novo para processar.")
            return

        # Aplica TRIM nas colunas de texto
        text_cols = ["motivo_devolucao", "status_devolucao"]
        df = transform.trim_columns(df, text_cols)
        
        # Extrai ano e mês
        df = transform.extract_date_parts(df, "data_devolucao")
        
        # Arredonda as colunas de valor para 2 casas decimais
        df = transform.round_values(df, ["valor_devolvido"], decimals=2)
        
        # Total de registros novos a serem gravados
        logger.info("Transformação executada. Total de registros novos a serem gravados: {}", df.count())

        # Grava os novos dados na camada Silver em modo append
        logger.info("Gravando novos dados transformados na Silver em: '{}'", silver_path)
        (
            df.write
            .mode("append")
            .partitionBy("ano", "mes", "status_devolucao")
            .parquet(silver_path)
        )
        
        logger.info("Job de transformação de devoluções finalizado com sucesso!")
        
    except Exception as e:
        logger.error("Falha durante o job de transformação de devoluções: {}", str(e))
        raise e
        
    finally:
        # Garante o encerramento da sessão
        close_spark_session(spark)

if __name__ == "__main__":
    run_transform_devolucoes()
