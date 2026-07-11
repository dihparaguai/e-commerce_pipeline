from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from loguru import logger

def create_total_without_discount(df: DataFrame) -> DataFrame:
    """
    Cria a coluna 'valor_total_sem_desconto'.
    Fórmula: valor_total + desconto
    """
    logger.info("Criando a coluna valor_total_sem_desconto")
    if df is None:
        raise ValueError("DataFrame não pode ser nulo")
    
    return df.withColumn(
        "valor_total_sem_desconto",
        F.col("valor_total") + F.col("desconto")
    )


