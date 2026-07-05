from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from loguru import logger

def _get_valid_columns(df: DataFrame, cols: list) -> list:
    """
    Função auxiliar para validar e retornar apenas as colunas que de fato existem no DataFrame.
    """
    valid_cols = [c for c in cols if c in df.columns]
    missing_cols = [c for c in cols if c not in valid_cols]
    if missing_cols:
        logger.warning("Colunas solicitadas mas não encontradas no DataFrame: {}", missing_cols)
    return valid_cols

def trim_columns(df: DataFrame, cols: list) -> DataFrame:
    """
    Aplica TRIM (remove espaços em branco no início e fim) nas colunas de texto selecionadas.
    """
    logger.info("Aplicando trim nas colunas: {}", cols)
    if not df or not cols:
        return df
    
    # Valida e filtra as colunas existentes
    valid_cols = _get_valid_columns(df, cols)
    for col in valid_cols:
        df = df.withColumn(col, F.trim(F.col(col)))
    return df

def fill_null_values(df: DataFrame, cols: list, fill_value: float = 0.0) -> DataFrame:
    """
    Substitui valores nulos nas colunas numéricas selecionadas por um valor padrão.
    """
    logger.info("Preenchendo nulos nas colunas {} com {}", cols, fill_value)
    if not df or not cols:
        return df
    
    # Valida e filtra as colunas existentes
    valid_cols = _get_valid_columns(df, cols)
    if valid_cols:
        df = df.fillna(fill_value, subset=valid_cols)
    return df

def extract_date_parts(df: DataFrame, date_col: str) -> DataFrame:
    """
    Extrai ano e mês a partir de uma coluna de data e adiciona como novas colunas.
    """
    logger.info("Extraindo ano e mês de data da coluna: {}", date_col)
    if not df or date_col not in df.columns:
        logger.warning("Coluna de data '{}' não encontrada no DataFrame.", date_col)
        return df
    
    # Cast para garantir o tipo Date antes da extração
    date_casted = F.to_date(F.col(date_col))
    df = (
        df
        .withColumn("ano", F.year(date_casted))
        .withColumn("mes", F.month(date_casted))
    )
    return df

def round_values(df: DataFrame, cols: list, decimals: int = 2) -> DataFrame:
    """
    Arredonda colunas para o número especificado de casas decimais.
    """
    logger.info("Arredondando colunas {} para {} casas decimais", cols, decimals)
    if not df or not cols:
        return df
    
    # Valida e filtra as colunas existentes
    valid_cols = _get_valid_columns(df, cols)
    for col in valid_cols:
        df = df.withColumn(col, F.round(F.col(col), decimals))
    return df

def capitalize_columns(df: DataFrame, cols: list) -> DataFrame:
    """
    Capitaliza (initcap) as colunas de texto especificadas.
    """
    logger.info("Capitalizando as colunas: {}", cols)
    if not df or not cols:
        return df
    
    # Valida e filtra as colunas existentes
    valid_cols = _get_valid_columns(df, cols)
    for col in valid_cols:
        df = df.withColumn(col, F.initcap(F.col(col)))
    return df

def filter_incremental(spark: SparkSession, df: DataFrame, silver_path: str, primary_key: str) -> DataFrame:
    """
    Aplica filtros incrementais comparando a data_carga_bronze e executando left_anti join com a Silver.
    """
    try:
        # Se os dados na camada Silver já existirem, prossegue com os filtros
        logger.info("Lendo dados existentes na camada Silver para validação incremental...")
        df_silver = spark.read.parquet(silver_path)
        
        # Filtro pela data_carga_bronze máxima presente na Silver
        max_date_row = df_silver.select(F.max("data_carga_bronze")).collect()[0]
        max_data_carga = max_date_row[0]
        
        # Se houver data de carga histórica, faz o filtro
        if max_data_carga:
            logger.info("Data máxima de carga encontrada na Silver: {}. Filtrando dados novos da Bronze...", max_data_carga)
            df = df.filter(F.col("data_carga_bronze") > max_data_carga)
            
        # Left anti join com a chave primária para garantir que registros já existentes não processem
        df = df.join(df_silver, on=primary_key, how="left_anti")
        logger.info("Filtros incrementais aplicados.")
        
    except Exception:
        # Caso a tabela de destino Silver ainda não exista, executa carga completa inicial
        logger.info("Camada Silver vazia ou não encontrada. Executando carga completa inicial.")
        
    return df
