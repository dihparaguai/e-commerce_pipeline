import sys

# Adiciona o diretório base (/opt/airflow) ao sys.path para reconhecer o módulo 'src'
sys.path.append("/opt/airflow")

from loguru import logger
from pyspark.sql import functions as F

from src.modules.spark_session import get_spark_session, close_spark_session
import src.modules.modeling_fato_utils as modeling_fato
import src.modules.utils as utils

def run_modeling_fato_vendas() -> None:
    """
    Job PySpark para ler a tabela Silver de vendas e a dim_local de Gold,
    gerar a tabela fato_vendas, e gravar o resultado na camada Gold.
    """
    logger.info("Iniciando o job de modelagem da Fato Vendas...")
    
    # Inicializa a sessão do Spark
    spark = get_spark_session("ModelingGoldFatoVendas")
    
    # Define caminhos de leitura e escrita
    silver_vendas_path = "s3a://silver/vendas"
    gold_dim_local_path = "s3a://gold/dim_local"
    gold_fato_vendas_path = "s3a://gold/fato_vendas"
    
    try:
        # Lê a tabela Silver de Vendas com try-except tolerante
        logger.info("Lendo Silver Vendas de: '{}'", silver_vendas_path)
        try:
            df_vendas = spark.read.parquet(silver_vendas_path)
        except Exception:
            logger.warning("Silver Vendas não encontrada. Ignorando esta origem.")
            df_vendas = None
            
        # Lê a tabela Gold dim_local com try-except tolerante
        logger.info("Lendo Gold dim_local de: '{}'", gold_dim_local_path)
        try:
            df_dim_local = spark.read.parquet(gold_dim_local_path)
        except Exception:
            logger.warning("Gold dim_local não encontrada. Ignorando esta origem.")
            df_dim_local = None
            
        # Processa a modelagem da fato_vendas
        df_fato_vendas = modeling_fato.create_fato_vendas(df_vendas, df_dim_local)
        
        if df_fato_vendas is None:
            logger.warning("Nenhuma fato de vendas processada (a tabela Silver de vendas estava ausente). Finalizando.")
            return
        
        total_records = df_fato_vendas.count()
        logger.info("Total de registros mapeados para a fato_vendas: {}", total_records)
        
        if total_records == 0:
            logger.warning("Nenhum registro encontrado para gravar na Gold.")
            return

        # Grava os dados na Gold sobrescrevendo a tabela anterior (overwrite)
        logger.info("Gravando a tabela fato_vendas na Gold em: '{}'", gold_fato_vendas_path)
        (
            df_fato_vendas.write
            .mode("overwrite")
            .partitionBy("ano", "mes", "status_pedido")
            .parquet(gold_fato_vendas_path)
        )
        logger.info("Job de modelagem da Fato Vendas finalizado com sucesso!")
        
    except Exception as e:
        logger.error("Falha durante o job de modelagem da Fato Vendas: {}", str(e))
        raise e
        
    finally:
        # Garante o fechamento da sessão Spark
        close_spark_session(spark)

if __name__ == "__main__":
    run_modeling_fato_vendas()
