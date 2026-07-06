import sys
from loguru import logger
from pyspark.sql import functions as F

# Adiciona o diretório base (/opt/airflow) ao sys.path para reconhecer o módulo 'src'
sys.path.append("/opt/airflow")
from src.services.spark_session import get_spark_session, close_spark_session
import src.modules.transform_utils as transform
import src.modules.utils as utils

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
        try:
            df = spark.read.parquet(bronze_path).withColumnRenamed("data_carga", "data_carga_bronze")
        except Exception:
            logger.warning("Camada Bronze '{}' vazia ou não encontrada. Nada para processar.", bronze_path)
            return
        
        # Filtros de Carga Incremental modularizados
        df = transform.filter_incremental(spark, df, silver_path, "devolucao_id")
        
        total_records = df.count()
        if total_records == 0:
            logger.info("Nenhum registro novo para processar.")
            return
        
        logger.info("Registros a serem processados nesta carga incremental: {}", total_records)
        
        # Aplica TRIM nas colunas de texto
        text_cols = ["motivo_devolucao", "status_devolucao"]
        df = transform.trim_columns(df, text_cols)
        
        # Extrai ano e mês
        df = transform.extract_date_parts(df, "data_devolucao")
        
        # Arredonda as colunas de valor para 2 casas decimais
        df = transform.round_values(df, ["valor_devolvido"], decimals=2)
        
        # Adiciona a data de carga do processamento da Silver
        df = df.withColumn("data_carga", F.to_date(F.lit(utils.get_current_date_str())))
        
        logger.info("Transformação executada. Total de registros novos a serem gravados: {}", df.count())

        # Grava os novos dados na camada Silver
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
