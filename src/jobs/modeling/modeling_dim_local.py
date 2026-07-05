import sys

# Adiciona o diretório base (/opt/airflow) ao sys.path para reconhecer o módulo 'src'
sys.path.append("/opt/airflow")

from loguru import logger
from pyspark.sql import functions as F

from src.modules.spark_session import get_spark_session, close_spark_session
import src.modules.modeling_dim_utils as modeling
import src.modules.utils as utils

def run_modeling_dim_local() -> None:
    """
    Job PySpark para ler dados da tabela Silver de vendas, gerar a Dimensão Local, e gravar o resultado na camada Gold.
    """
    logger.info("Iniciando o job de modelagem da Dimensão Local...")
    
    # Inicializa a sessão do Spark
    spark = get_spark_session("ModelingGoldDimLocal")
    
    # Define caminhos de leitura na Silver e escrita na Gold
    silver_vendas_path = "s3a://silver/vendas"
    gold_path = "s3a://gold/dim_local"
    
    try:
        # Lê a tabela Silver de Vendas com try-except tolerante
        logger.info("Lendo Silver Vendas de: '{}'", silver_vendas_path)
        try:
            df_vendas = spark.read.parquet(silver_vendas_path)
        except Exception:
            logger.warning("Silver Vendas não encontrada. Ignorando esta origem.")
            df_vendas = None
            
        # Processa a modelagem da dim_local passando vendas (que pode ser None)
        df_dim_local = modeling.create_dim_local(df_vendas)
        
        if df_dim_local is None:
            logger.warning("Nenhum local processado (a tabela Silver de vendas estava ausente). Finalizando.")
            return
        
        # Adiciona a data de carga com timezone America/Sao_Paulo
        df_dim_local = df_dim_local.withColumn("data_carga", F.to_date(F.lit(utils.get_current_date_str())))
        
        total_records = df_dim_local.count()
        logger.info("Total de locais mapeados para a dim_local: {}", total_records)
        
        if total_records == 0:
            logger.warning("Nenhum local encontrado para gravar na Gold.")
            return

        # Grava os dados na Gold sobrescrevendo a tabela anterior (overwrite)
        logger.info("Gravando a tabela dim_local na Gold em: '{}'", gold_path)
        (
            df_dim_local.write
            .mode("overwrite")
            .parquet(gold_path)
        )
        logger.info("Job de modelagem da Dimensão Local finalizado com sucesso!")
        
    except Exception as e:
        logger.error("Falha durante o job de modelagem da Dimensão Local: {}", str(e))
        raise e
        
    finally:
        # Garante o fechamento da sessão Spark
        close_spark_session(spark)

if __name__ == "__main__":
    run_modeling_dim_local()
