import sys

# Adiciona o diretório base (/opt/airflow) ao sys.path para reconhecer o módulo 'src'
sys.path.append("/opt/airflow")

from loguru import logger
from pyspark.sql import functions as F

from src.services.spark_session import get_spark_session, close_spark_session
from src.modules.dw_loader import load_to_postgres
import src.modules.modeling_fato_utils as modeling_fato
import src.modules.utils as utils

def run_modeling_fato_devolucoes() -> None:
    """
    Job PySpark para ler a tabela Silver de devoluções,
    gerar a tabela fato_devolucoes, e gravar o resultado na camada Gold.
    """
    logger.info("Iniciando o job de modelagem da Fato Devoluções...")
    
    # Inicializa a sessão do Spark
    spark = get_spark_session("ModelingGoldFatoDevolucoes")
    
    # Define caminhos de leitura e escrita
    silver_devolucoes_path = "s3a://silver/devolucoes"
    gold_fato_devolucoes_path = "s3a://gold/fato_devolucoes"
    
    try:
        # Lê a tabela Silver de Devoluções com try-except tolerante
        logger.info("Lendo Silver Devoluções de: '{}'", silver_devolucoes_path)
        try:
            df_devolucoes = spark.read.parquet(silver_devolucoes_path)
        except Exception:
            logger.warning("Silver Devolucoes não encontrada. Ignorando esta origem.")
            df_devolucoes = None
            
        # Processa a modelagem da fato_devolucoes
        df_fato_devolucoes = modeling_fato.create_fato_devolucoes(df_devolucoes)
        
        if df_fato_devolucoes is None:
            logger.warning("Nenhuma fato de devoluções processada (a tabela Silver de devoluções estava ausente). Finalizando.")
            return
        
        total_records = df_fato_devolucoes.count()
        logger.info("Total de registros mapeados para a fato_devolucoes: {}", total_records)
        
        if total_records == 0:
            logger.warning("Nenhum registro encontrado para gravar na Gold.")
            return

        # Grava os dados na Gold sobrescrevendo a tabela anterior (overwrite)
        logger.info("Gravando a tabela fato_devolucoes na Gold em: '{}'", gold_fato_devolucoes_path)
        (
            df_fato_devolucoes.write
            .mode("overwrite")
            .parquet(gold_fato_devolucoes_path)
        )
        
        # Executa a carga no PostgreSQL DW
        df_to_load = df_fato_devolucoes.select(
            "devolucao_id", "pedido_id", "produto_id", "cliente_id", "data_devolucao", 
            "motivo_devolucao", "status_devolucao", "valor_devolvido"
        )
        load_to_postgres(df=df_to_load, table_name="fato_devolucoes")
        
        logger.info("Job de modelagem da Fato Devoluções finalizado com sucesso!")
        
    except Exception as e:
        logger.error("Falha durante o job de modelagem da Fato Devoluções: {}", str(e))
        raise e
        
    finally:
        # Garante o fechamento da sessão Spark
        close_spark_session(spark)

if __name__ == "__main__":
    run_modeling_fato_devolucoes()
