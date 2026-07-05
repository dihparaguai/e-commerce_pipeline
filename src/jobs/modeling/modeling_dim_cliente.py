import sys

# Adiciona o diretório base (/opt/airflow) ao sys.path para reconhecer o módulo 'src'
sys.path.append("/opt/airflow")

from loguru import logger
from pyspark.sql import functions as F

from src.modules.spark_session import get_spark_session, close_spark_session
import src.modules.modeling_dim_utils as modeling
import src.modules.utils as utils

def run_modeling_dim_cliente() -> None:
    """
    Job PySpark para ler dados das tabelas Silver (vendas, devoluções), gerar a Dimensão Cliente, e gravar o resultado na camada Gold.
    """
    logger.info("Iniciando o job de modelagem da Dimensão Cliente...")
    
    # Inicializa a sessão do Spark
    spark = get_spark_session("ModelingGoldDimCliente")
    
    # Define caminhos de leitura na Silver e escrita na Gold
    silver_vendas_path = "s3a://silver/vendas"
    silver_devolucoes_path = "s3a://silver/devolucoes"
    gold_path = "s3a://gold/dim_cliente"
    
    try:
        # Lê a tabela Silver de Vendas com try-except tolerante
        logger.info("Lendo Silver Vendas de: '{}'", silver_vendas_path)
        try:
            df_vendas = spark.read.parquet(silver_vendas_path)
        except Exception:
            logger.warning("Silver Vendas não encontrada. Ignorando esta origem.")
            df_vendas = None
            
        # Lê a tabela Silver de Devoluções com try-except tolerante
        logger.info("Lendo Silver Devoluções de: '{}'", silver_devolucoes_path)
        try:
            df_devolucoes = spark.read.parquet(silver_devolucoes_path)
        except Exception:
            logger.warning("Silver Devoluções não encontrada. Ignorando esta origem.")
            df_devolucoes = None
            
        # Processa a modelagem da dim_cliente passando as variáveis (que podem ser None)
        df_dim_cliente = modeling.create_dim_cliente(df_vendas, df_devolucoes)
        
        if df_dim_cliente is None:
            logger.warning("Nenhum cliente processado (todas as tabelas Silver de origem estavam ausentes). Finalizando.")
            return
        
        # Adiciona a data de carga com timezone America/Sao_Paulo
        df_dim_cliente = df_dim_cliente.withColumn("data_carga", F.to_date(F.lit(utils.get_current_date_str())))
        
        total_records = df_dim_cliente.count()
        logger.info("Total de clientes mapeados para a dim_cliente: {}", total_records)
        
        if total_records == 0:
            logger.warning("Nenhum cliente encontrado para gravar na Gold.")
            return

        # Grava os dados na Gold sobrescrevendo a tabela anterior (overwrite)
        logger.info("Gravando a tabela dim_cliente na Gold em: '{}'", gold_path)
        (
            df_dim_cliente.write
            .mode("overwrite")
            .parquet(gold_path)
        )
        logger.info("Job de modelagem da Dimensão Cliente finalizado com sucesso!")
        
    except Exception as e:
        logger.error("Falha durante o job de modelagem da Dimensão Cliente: {}", str(e))
        raise e
        
    finally:
        # Garante o fechamento da sessão Spark
        close_spark_session(spark)

if __name__ == "__main__":
    run_modeling_dim_cliente()
