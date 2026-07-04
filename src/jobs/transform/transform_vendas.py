
import sys

# Adiciona o diretório base (/opt/airflow) ao sys.path para reconhecer o módulo 'src'
sys.path.append("/opt/airflow")

from loguru import logger
from pyspark.sql import functions as F

from src.modules.spark_session import get_spark_session, close_spark_session
import src.modules.transform_utils as transform
import src.modules.transform_vendas_utils as transform_vendas

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
                
            # Left anti join com a chave primária pedido_id
            df = df.join(df_silver, on="pedido_id", how="left_anti")
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
        
        # Extrai ano, mês e dia
        df = transform.extract_date_parts(df, "data_pedido")
        
        # Arredonda as colunas de valor para 2 casas decimais
        all_value_cols = value_cols + ["valor_total_sem_desconto"]
        df = transform.round_values(df, all_value_cols, decimals=2)
        
        # Total de registros novos a serem gravados
        logger.info("Transformação executada. Total de registros novos a serem gravados: {}", df.count())

        # Grava os novos dados na camada Silver em modo append
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
