import sys

# Adiciona o diretório base (/opt/airflow) ao sys.path para reconhecer o módulo 'src'
sys.path.append("/opt/airflow")

from loguru import logger
from pyspark.sql import functions as F

from src.services.spark_session import get_spark_session, close_spark_session
from src.modules.dw_loader import load_to_postgres
import src.modules.modeling_dim_utils as modeling
import src.modules.utils as utils

def run_modeling_dim_fornecedor() -> None:
    """
    Job PySpark para ler dados da tabela Silver de estoque, gerar a Dimensão Fornecedor, e gravar o resultado na camada Gold.
    """
    logger.info("Iniciando o job de modelagem da Dimensão Fornecedor...")
    
    # Inicializa a sessão do Spark
    spark = get_spark_session("ModelingGoldDimFornecedor")
    
    # Define caminhos de leitura na Silver e escrita na Gold
    silver_estoque_path = "s3a://silver/estoque"
    gold_path = "s3a://gold/dim_fornecedor"
    
    try:
        # Lê a tabela Silver de Estoque com try-except tolerante
        logger.info("Lendo Silver Estoque de: '{}'", silver_estoque_path)
        try:
            df_estoque = spark.read.parquet(silver_estoque_path)
        except Exception:
            logger.warning("Silver Estoque não encontrada. Ignorando esta origem.")
            df_estoque = None
            
        # Processa a modelagem da dim_fornecedor passando estoque (que pode ser None)
        df_dim_fornecedor = modeling.create_dim_fornecedor(df_estoque)
        
        if df_dim_fornecedor is None:
            logger.warning("Nenhum fornecedor processado (a tabela Silver de estoque estava ausente). Finalizando.")
            return
        
        # Adiciona a data de carga com timezone America/Sao_Paulo
        df_dim_fornecedor = df_dim_fornecedor.withColumn("data_carga", F.to_date(F.lit(utils.get_current_date_str())))
        
        total_records = df_dim_fornecedor.count()
        logger.info("Total de fornecedores mapeados para a dim_fornecedor: {}", total_records)
        
        if total_records == 0:
            logger.warning("Nenhum fornecedor encontrado para gravar na Gold.")
            return

        # Grava os dados na Gold sobrescrevendo a tabela anterior (overwrite)
        logger.info("Gravando a tabela dim_fornecedor na Gold em: '{}'", gold_path)
        (
            df_dim_fornecedor.write
            .mode("overwrite")
            .parquet(gold_path)
        )
        
        # Executa a carga no PostgreSQL DW
        df_to_load = df_dim_fornecedor.select("fornecedor_id", "fornecedor")
        load_to_postgres(df=df_to_load, table_name="dim_fornecedor")
        
        logger.info("Job de modelagem da Dimensão Fornecedor finalizado com sucesso!")
        
    except Exception as e:
        logger.error("Falha durante o job de modelagem da Dimensão Fornecedor: {}", str(e))
        raise e
        
    finally:
        # Garante o fechamento da sessão Spark
        close_spark_session(spark)

if __name__ == "__main__":
    run_modeling_dim_fornecedor()
