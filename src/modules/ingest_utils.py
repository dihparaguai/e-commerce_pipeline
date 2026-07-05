import os
import pandas as pd
from loguru import logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

def get_processed_files(log_dir: str, log_filename: str) -> list:
    """
    Lê o log de CDC (CSV) usando pandas e retorna uma lista (list) de nomes de arquivos já processados.
    """
    # Garante a existência do diretório do log
    os.makedirs(log_dir, exist_ok=True)
    log_filepath = os.path.join(log_dir, log_filename)
    
    # Se o arquivo não existir, cria um DataFrame vazio com cabeçalho
    if not os.path.exists(log_filepath):
        logger.info("Log de CDC não encontrado em {}. Criando novo log.", log_filepath)
        df = pd.DataFrame(columns=["nome_arquivo", "data_carga"])
        df.to_csv(log_filepath, index=False)
        return []
        
    try:
        # Lê o CSV e retorna a coluna de arquivos processados como uma lista
        df = pd.read_csv(log_filepath)
        processed = df["nome_arquivo"].dropna().astype(str).tolist()
    except Exception as e:
        logger.error("Erro ao ler o arquivo de log CDC com pandas: {}. Retornando conjunto vazio.", str(e))
        processed = []
    
    return processed

def register_processed_files(log_dir: str, log_filename: str, files: list, data_carga: str) -> None:
    """
    Registra uma lista de arquivos processados e a data de carga no log de CDC (CSV) usando pandas de uma só vez.
    """
    # Garante a existência do diretório
    os.makedirs(log_dir, exist_ok=True)
    log_filepath = os.path.join(log_dir, log_filename)
    
    # Cria novas linhas para anexar
    new_rows = pd.DataFrame([{"nome_arquivo": f, "data_carga": data_carga} for f in files])
    try:
        if not os.path.exists(log_filepath):
            new_rows.to_csv(log_filepath, index=False)
        else:
            new_rows.to_csv(log_filepath, mode="a", header=False, index=False)
        logger.info("Registrado no log de CDC: {}", files)
    except Exception as e:
        logger.error("Erro ao escrever no arquivo de log CDC com pandas: {}", str(e))
        raise e

def get_new_files(raw_dir: str, log_dir: str, log_filename: str) -> list:
    """
    Retorna uma lista de arquivos CSV no diretório raw que não foram processados com base no log do CDC.
    """
    # Obtém conjunto de arquivos que já foram ingeridos
    processed_files = get_processed_files(log_dir, log_filename)
    
    # Valida se o diretório existe
    if not os.path.exists(raw_dir):
        logger.warning("Diretório de origem não existe: {}", raw_dir)
        return []
    
    # Filtra os arquivos CSV que não constam no conjunto de processados
    all_files = [f for f in os.listdir(raw_dir) if f.endswith(".csv")]
    new_files = [f for f in all_files if f not in processed_files]
    return new_files

def read_new_csv_files(spark: SparkSession, raw_dir: str, new_files: list, data_carga_str: str) -> DataFrame:
    """
    Lê novos arquivos CSV em um DataFrame do Spark e adiciona a coluna data_carga formatada.
    """
    new_filepaths = [os.path.join(raw_dir, f) for f in new_files]
    
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(new_filepaths)
    )
    
    # Converte e adiciona a coluna data_carga no formato de DateType
    return df.withColumn("data_carga", F.to_date(F.lit(data_carga_str)))

def deduplicate_and_filter_existing(spark: SparkSession, df: DataFrame, primary_key: str, bronze_parquet_path: str) -> DataFrame:
    """
    Remove registros duplicados da própria carga nova e filtra IDs que já existem no histórico da Bronze.
    """
    try:
        logger.info("Buscando dados existentes no destino para realizar o filtro incremental...")
        df_existing = spark.read.parquet(bronze_parquet_path)
        
        # Deduplica chaves locais na nova carga
        df_new_unique = df.dropDuplicates([primary_key])
        
        # Filtra os registros que já existem na Bronze
        df_to_append = df_new_unique.join(df_existing, on=primary_key, how="left_anti")
        logger.info("Dados existentes encontrados. Filtrando chaves duplicadas.")
        
    except Exception:
        # Caso seja a primeira carga, apenas remove duplicados locais
        logger.info("Nenhum dado existente encontrado. Preparando primeira carga.")
        df_to_append = df.dropDuplicates([primary_key])
    return df_to_append
