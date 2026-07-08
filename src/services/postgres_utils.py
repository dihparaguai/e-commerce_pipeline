import os
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger

def get_postgres_connection():
    """
    Carrega as variáveis de ambiente e estabelece uma conexão com o PostgreSQL, testando a conectividade. Retorna o objeto de conexão ativo.
    """
    logger.info("Iniciando conexão com o PostgreSQL...")
    load_dotenv()
    
    # Obtém parâmetros do .env
    pg_user = os.getenv("PG_USER")
    pg_password = os.getenv("PG_PASSWORD")
    pg_host = os.getenv("PG_HOST")
    pg_port = os.getenv("PG_PORT")
    pg_db = os.getenv("PG_DB")

    # Garante que as variáveis não sejam nulas ou strings vazias
    if not pg_user or not pg_host or not pg_db:
        raise ValueError("Variáveis de ambiente do PostgreSQL (PG_USER, PG_HOST, PG_DB) não estão configuradas ou estão vazias no .env.")

    try:
        logger.info(f"Conectando ao banco '{pg_db}' em '{pg_host}:{pg_port}' como '{pg_user}'...")
        conn = psycopg2.connect(
            dbname=pg_db,
            user=pg_user,
            password=pg_password,
            host=pg_host,
            port=pg_port
        )
        conn.autocommit = True # Garante que as alterações sejam salvas automaticamente
        
        # Executa query de teste de conectividade
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1;")
            cursor.fetchone()
            
        logger.info("Conexão estabelecida e testada com sucesso (SELECT 1)!")
        return conn
    except Exception as e:
        logger.error(f"Falha ao conectar no PostgreSQL: {e}")
        raise e

def execute_sql_script(file_path: str | Path) -> None:
    """
    Função que lê um arquivo SQL e executa suas instruções no banco.
    """
    path = Path(file_path)
    logger.info(f"Iniciando a execução do script SQL: '{path}'")

    if not path.exists():
        raise FileNotFoundError(f"Arquivo SQL não encontrado no caminho: {path}")

    try:
        # Lê o conteúdo SQL
        with open(path, "r", encoding="utf-8") as f:
            sql_script = f.read()

        # Estabelece a conexão com o banco de dados e cria o cursor
        conn = get_postgres_connection()
        
        # O bloco 'with' abre e fecha o cursor automaticamente
        with conn.cursor() as cursor:
            logger.info(f"Executando script '{path.name}'...")
            cursor.execute(sql_script)

        # Fecha a conexão com o banco de dados
        conn.close()
        logger.info(f"Script '{path.name}' executado com sucesso no PostgreSQL!")
    except Exception as e:
        logger.error(f"Falha ao executar o script SQL '{path.name}'. Detalhes: {e}")
        raise e

def truncate_table(table_name: str) -> None:
    """
    Executa um TRUNCATE TABLE CASCADE na tabela especificada do banco de dados.
    """
    logger.info(f"Preparando TRUNCATE CASCADE na tabela '{table_name}'...")
    try:
        conn = get_postgres_connection()
        with conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
        conn.close()
        logger.info(f"Tabela '{table_name}' truncada com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao tentar executar TRUNCATE CASCADE na tabela '{table_name}': {e}")
