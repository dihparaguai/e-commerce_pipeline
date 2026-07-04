from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from loguru import logger

def trim_columns(df: DataFrame, cols: list) -> DataFrame:
    """
    Aplica TRIM (remove espaços em branco) nas colunas de texto selecionadas.
    """
    logger.info("Aplicando trim nas colunas: {}", cols)
    if not df or not cols:
        return df
    for col in cols:
        if col in df.columns:
            df = df.withColumn(col, F.trim(F.col(col)))
    return df

def fill_null_values(df: DataFrame, cols: list, fill_value: float = 0.0) -> DataFrame:
    """
    Substitui valores nulos nas colunas numéricas por um valor padrão.
    """
    logger.info("Preenchendo nulos nas colunas {} com {}", cols, fill_value)
    if not df or not cols:
        return df
    valid_cols = [c for c in cols if c in df.columns]
    if valid_cols:
        df = df.fillna(fill_value, subset=valid_cols)
    return df

def extract_date_parts(df: DataFrame, date_col: str) -> DataFrame:
    """
    Extrai ano, mês e dia a partir de uma coluna de data.
    """
    logger.info("Extraindo partes de data da coluna: {}", date_col)
    if not df or date_col not in df.columns:
        return df
    date_casted = F.to_date(F.col(date_col))
    df = (
        df
        .withColumn("ano", F.year(date_casted))
        .withColumn("mes", F.month(date_casted))
        .withColumn("dia", F.dayofmonth(date_casted))
    )
    return df

def round_values(df: DataFrame, cols: list, decimals: int = 2) -> DataFrame:
    """
    Arredonda colunas para o número especificado de casas decimais.
    """
    logger.info("Arredondando colunas {} para {} casas", cols, decimals)
    if not df or not cols:
        return df
    for col in cols:
        if col in df.columns:
            df = df.withColumn(col, F.round(F.col(col), decimals))
    return df

def capitalize_columns(df: DataFrame, cols: list) -> DataFrame:
    """
    Capitaliza (initcap) as colunas de texto especificadas.
    """
    logger.info("Capitalizando as colunas: {}", cols)
    if not df or not cols:
        return df
    for col in cols:
        if col in df.columns:
            df = df.withColumn(col, F.initcap(F.col(col)))
    return df
