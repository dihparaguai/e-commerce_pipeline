import sys
from loguru import logger
from pyspark.sql import functions as F

# Adiciona o diretório base (/opt/airflow) ao sys.path para reconhecer o módulo 'src'
sys.path.append("/opt/airflow")
from src.modules.spark_session import get_spark_session, close_spark_session
import src.modules.modeling_dim_utils as modeling
import src.modules.utils as utils

def run_modeling_dim_produto() -> None:
    """
    Job PySpark para ler dados das tabelas Silver (vendas, estoque, devoluções), gerar a Dimensão Produto, e gravar o resultado na camada Gold.
    """
    logger.info("Iniciando o job de modelagem da Dimensão Produto...")
    
    # Inicializa a sessão do Spark
    spark = get_spark_session("ModelingGoldDimProduto")
    
    # Define caminhos de leitura na Silver e escrita na Gold
    silver_vendas_path = "s3a://silver/vendas"
    silver_estoque_path = "s3a://silver/estoque"
    silver_devolucoes_path = "s3a://silver/devolucoes"
    gold_path = "s3a://gold/dim_produto"
    
    try:
        # Lê as tabelas Silver de origem
        logger.info("Lendo Silver Vendas de: '{}'", silver_vendas_path)
        try:
            df_vendas = spark.read.parquet(silver_vendas_path)
        except Exception:
            logger.warning("Silver Vendas não encontrada. Ignorando esta origem.")
            df_vendas = None
            
        logger.info("Lendo Silver Estoque de: '{}'", silver_estoque_path)
        try:
            df_estoque = spark.read.parquet(silver_estoque_path)
        except Exception:
            logger.warning("Silver Estoque não encontrada. Ignorando esta origem.")
            df_estoque = None
            
        logger.info("Lendo Silver Devoluções de: '{}'", silver_devolucoes_path)
        try:
            df_devolucoes = spark.read.parquet(silver_devolucoes_path)
        except Exception:
            logger.warning("Silver Devoluções não encontrada. Ignorando esta origem.")
            df_devolucoes = None
            
        # Processa a modelagem da dim_produto passando as variáveis (que podem ser None)
        df_dim_produto = modeling.create_dim_produto(df_vendas, df_estoque, df_devolucoes)
        
        if df_dim_produto is None:
            logger.warning("Nenhum produto processado (todas as tabelas Silver de origem estavam ausentes). Finalizando.")
            return
        
        # Adiciona a data de carga com timezone America/Sao_Paulo
        df_dim_produto = df_dim_produto.withColumn("data_carga", F.to_date(F.lit(utils.get_current_date_str())))
        
        total_records = df_dim_produto.count()
        logger.info("Total de produtos mapeados para a dim_produto: {}", total_records)
        
        if total_records == 0:
            logger.warning("Nenhum produto encontrado para gravar na Gold.")
            return

        # Grava os dados na Gold sobrescrevendo a tabela anterior (overwrite)
        logger.info("Gravando a tabela dim_produto na Gold em: '{}'", gold_path)
        (
            df_dim_produto.write
            .mode("overwrite")
            .partitionBy("categoria")
            .parquet(gold_path)
        )
        logger.info("Job de modelagem da Dimensão Produto finalizado com sucesso!")
        
    except Exception as e:
        logger.error("Falha durante o job de modelagem da Dimensão Produto: {}", str(e))
        raise e
        
    finally:
        # Garante o fechamento da sessão Spark
        close_spark_session(spark)

if __name__ == "__main__":
    run_modeling_dim_produto()
