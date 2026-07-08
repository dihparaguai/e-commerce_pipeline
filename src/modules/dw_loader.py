import os
from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import DataFrame
from src.services.postgres_utils import truncate_table

def load_to_postgres(df: DataFrame, table_name: str) -> None:
    """
    Carrega um DataFrame do Spark em uma tabela específica do PostgreSQL DW usando Spark JDBC.
    """
    logger.info(f"Iniciando a carga de dados para a tabela '{table_name}' no PostgreSQL DW via Spark JDBC...")
    load_dotenv()
    
    # Carrega credenciais do .env
    pg_user = os.getenv("PG_USER")
    pg_password = os.getenv("PG_PASSWORD")
    pg_host = os.getenv("PG_HOST")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db = os.getenv("PG_DB")

    if not pg_user or not pg_host or not pg_db:
        raise ValueError("Configurações do PostgreSQL (PG_USER, PG_HOST, PG_DB) não foram encontradas no .env.")

    # Limpa a tabela antes de carregar, isso evita que o Spark tente dar DROP TABLE (no modo overwrite) e quebre as chaves estrangeiras.
    truncate_table(table_name)

    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    try:
        logger.info(f"Gravando dados na tabela '{table_name}' via JDBC...")
        (
            df.write
            .mode("append") # Append mantém as PKs e FKs intactas
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table_name)
            .option("user", pg_user)
            .option("password", pg_password)
            .option("driver", "org.postgresql.Driver")
            .save()
        )
        logger.info(f"Carga da tabela '{table_name}' no PostgreSQL DW concluída com sucesso!")
    except Exception as e:
        logger.error(f"Erro ao carregar dados na tabela '{table_name}' via JDBC: {e}")
        raise e
