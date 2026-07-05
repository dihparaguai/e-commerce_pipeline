import sys
from loguru import logger
from pyspark.sql import functions as F

# Adiciona o diretório base (/opt/airflow) ao sys.path para reconhecer o módulo 'src'
sys.path.append("/opt/airflow")
from src.modules.spark_session import get_spark_session, close_spark_session
import src.modules.transform_utils as transform
import src.modules.transform_vendas_utils as transform_vendas
import src.modules.utils as utils

def run_transform_vendas() -> None:
    """
    Job PySpark para ler a base de vendas na camada Bronze, aplicar filtros incrementais, transformações, e gravar o resultado na camada Silver no MinIO.
    """
    logger.info("Iniciando o job de transformação de vendas (Bronze -> Silver)...")
    
    # Inicializa a sessão do Spark
    spark = get_spark_session("TransformVendasSilver")
    
    # Define os caminhos de origem e destino no MinIO
    bronze_path = "s3a://bronze/vendas"
    silver_path = "s3a://silver/vendas"
    
    try:
        # Lê os dados da camada Bronze
        logger.info("Lendo dados da camada Bronze de: '{}'", bronze_path)
        try:
            df = spark.read.parquet(bronze_path).withColumnRenamed("data_carga", "data_carga_bronze")
        except Exception:
            logger.warning("Camada Bronze '{}' vazia ou não encontrada. Nada para processar.", bronze_path)
            return
        
        # Filtros de Carga Incremental modularizados
        df = transform.filter_incremental(spark, df, silver_path, "pedido_id")
        
        total_records = df.count()
        if total_records == 0:
            logger.info("Nenhum registro novo para processar.")
            return
        
        logger.info("Registros a serem processados nesta carga incremental: {}", total_records)
        
        # Aplica TRIM nas colunas de texto
        text_cols = [
            "produto", "categoria", "marca", "canal_venda", 
            "forma_pagamento", "cidade", "estado", "status_pedido"
        ]
        df = transform.trim_columns(df, text_cols)
        
        # Preenche nulos nas colunas de valor por 0
        value_cols = ["preco_unitario", "desconto", "frete", "valor_total"]
        df = transform.fill_null_values(df, value_cols, fill_value=0.0)
        
        # Cria coluna 'valor_total_sem_desconto'
        df = transform_vendas.create_total_without_discount(df)
        
        # Capitaliza colunas de texto
        df = transform.capitalize_columns(df, ["cidade"])
        
        # Extrai ano e mês
        df = transform.extract_date_parts(df, "data_pedido")
        
        # Arredonda as colunas de valor para 2 casas decimais
        all_value_cols = value_cols + ["valor_total_sem_desconto"]
        df = transform.round_values(df, all_value_cols, decimals=2)
        
        # Adiciona a data de carga do processamento da Silver
        df = df.withColumn("data_carga", F.to_date(F.lit(utils.get_current_date_str())))
        
        logger.info("Transformação executada. Total de registros novos a serem gravados: {}", df.count())

        # Grava os novos dados na camada Silver
        logger.info("Gravando novos dados transformados na Silver em: '{}'", silver_path)
        (
            df.write
            .mode("append")
            .partitionBy("ano", "mes", "estado", "status_pedido")
            .parquet(silver_path)
        )
        
        logger.info("Job de transformação de vendas finalizado com sucesso!")
        
    except Exception as e:
        logger.error("Falha durante o job de transformação de vendas: {}", str(e))
        raise e
        
    finally:
        # Garante o encerramento da sessão
        close_spark_session(spark)

if __name__ == "__main__":
    run_transform_vendas()
