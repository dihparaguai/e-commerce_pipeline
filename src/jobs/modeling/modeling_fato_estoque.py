import sys

# Adiciona o diretório base (/opt/airflow) ao sys.path para reconhecer o módulo 'src'
sys.path.append("/opt/airflow")

from loguru import logger
from pyspark.sql import functions as F

from src.modules.spark_session import get_spark_session, close_spark_session
import src.modules.modeling_fato_utils as modeling_fato
import src.modules.utils as utils

def run_modeling_fato_estoque() -> None:
    """
    Job PySpark para ler a tabela Silver de estoque e a dim_fornecedor de Gold,
    gerar a tabela fato_estoque, e gravar o resultado na camada Gold.
    """
    logger.info("Iniciando o job de modelagem da Fato Estoque...")
    
    # Inicializa a sessão do Spark
    spark = get_spark_session("ModelingGoldFatoEstoque")
    
    # Define caminhos de leitura e escrita
    silver_estoque_path = "s3a://silver/estoque"
    gold_dim_fornecedor_path = "s3a://gold/dim_fornecedor"
    gold_fato_estoque_path = "s3a://gold/fato_estoque"
    
    try:
        # Lê a tabela Silver de Estoque com try-except tolerante
        logger.info("Lendo Silver Estoque de: '{}'", silver_estoque_path)
        try:
            df_estoque = spark.read.parquet(silver_estoque_path)
        except Exception:
            logger.warning("Silver Estoque não encontrada. Ignorando esta origem.")
            df_estoque = None
            
        # Lê a tabela Gold dim_fornecedor com try-except tolerante
        logger.info("Lendo Gold dim_fornecedor de: '{}'", gold_dim_fornecedor_path)
        try:
            df_dim_fornecedor = spark.read.parquet(gold_dim_fornecedor_path)
        except Exception:
            logger.warning("Gold dim_fornecedor não encontrada. Ignorando esta origem.")
            df_dim_fornecedor = None
            
        # Processa a modelagem da fato_estoque
        df_fato_estoque = modeling_fato.create_fato_estoque(df_estoque, df_dim_fornecedor)
        
        if df_fato_estoque is None:
            logger.warning("Nenhuma fato de estoque processada (a tabela Silver de estoque estava ausente). Finalizando.")
            return
        
        # Adiciona a data de carga com timezone America/Sao_Paulo
        df_fato_estoque = df_fato_estoque.withColumn("data_carga", F.to_date(F.lit(utils.get_current_date_str())))
        
        total_records = df_fato_estoque.count()
        logger.info("Total de registros mapeados para a fato_estoque: {}", total_records)
        
        if total_records == 0:
            logger.warning("Nenhum registro encontrado para gravar na Gold.")
            return

        # Grava os dados na Gold sobrescrevendo a tabela anterior (overwrite)
        logger.info("Gravando a tabela fato_estoque na Gold em: '{}'", gold_fato_estoque_path)
        (
            df_fato_estoque.write
            .mode("overwrite")
            .parquet(gold_fato_estoque_path)
        )
        logger.info("Job de modelagem da Fato Estoque finalizado com sucesso!")
        
    except Exception as e:
        logger.error("Falha durante o job de modelagem da Fato Estoque: {}", str(e))
        raise e
        
    finally:
        # Garante o fechamento da sessão Spark
        close_spark_session(spark)

if __name__ == "__main__":
    run_modeling_fato_estoque()
