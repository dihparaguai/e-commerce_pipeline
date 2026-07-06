from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from loguru import logger

def _drop_data_carga(df: DataFrame) -> DataFrame:
    """
    Remove colunas de controle de data de carga (como data_carga e data_carga_bronze)
    que existirem no DataFrame para evitar duplicações e conflitos de nomes.
    """
    if df is None:
        return None
    # Identifica quais colunas de data de carga estão presentes no DataFrame
    cols_to_drop = [c for c in ["data_carga", "data_carga_bronze"] if c in df.columns]
    if cols_to_drop:
        logger.debug("Removendo colunas de data de carga obsoletas: {}", cols_to_drop)
        df = df.drop(*cols_to_drop)
    return df

def create_fato_vendas(df_vendas: DataFrame, df_dim_local: DataFrame) -> DataFrame:
    """
    Cria a tabela fato_vendas a partir da tabela Silver de vendas e da dimensão local (dim_local).
    Realiza o join para trazer o local_id e remove colunas descritivas (cidade, estado, produto, categoria, marca).
    """
    logger.info("Iniciando a modelagem da fato vendas (fato_vendas)...")
    
    if df_vendas is None:
        logger.warning("Origem de vendas é None. Retornando None.")
        return None
        
    # Limpa as colunas de data_carga das origens para evitar duplicação no join
    df_vendas = _drop_data_carga(df_vendas)
    
    if df_dim_local is None:
        logger.warning("Dimensão local é None. Preenchendo local_id com nulo.")
        df_joined = df_vendas.withColumn("local_id", F.lit(None).cast("int"))
    else:
        df_dim_local = _drop_data_carga(df_dim_local)
        
        # Realiza o join para associar o local_id correspondente à cidade/estado
        logger.info("Realizando join com a dim_local para buscar o local_id...")
        df_joined = df_vendas.join(df_dim_local, on=["cidade", "estado"], how="left")
        
    # Remove colunas redundantes que já estão contidas nas dimensões (dim_produto e dim_local)
    logger.info("Removendo colunas descritivas redundantes de produto e localização...")
    cols_to_drop = ["produto", "categoria", "marca", "cidade", "estado"]
    df_fato = df_joined.drop(*cols_to_drop)
    
    logger.info("Modelagem da fato_vendas concluída.")
    return df_fato

def create_fato_devolucoes(df_devolucoes: DataFrame) -> DataFrame:
    """
    Cria a tabela fato_devolucoes a partir da tabela Silver de devoluções.
    Remove colunas de controle de carga anteriores.
    """
    logger.info("Iniciando a modelagem da fato devoluções (fato_devolucoes)...")
    if df_devolucoes is None:
        logger.warning("Origem de devoluções é None. Retornando None.")
        return None
        
    df_fato = _drop_data_carga(df_devolucoes)
    logger.info("Modelagem da fato_devolucoes concluída.")
    return df_fato

def create_fato_estoque(df_estoque: DataFrame, df_dim_fornecedor: DataFrame) -> DataFrame:
    """
    Cria a tabela fato_estoque a partir da tabela Silver de estoque e da dimensão fornecedor (dim_fornecedor).
    Realiza o join para trazer o fornecedor_id e remove colunas descritivas (produto, categoria, marca, fornecedor).
    """
    logger.info("Iniciando a modelagem da fato estoque (fato_estoque)...")
    if df_estoque is None:
        logger.warning("Origem de estoque é None. Retornando None.")
        return None
        
    # Limpa colunas de data_carga antigas
    df_estoque = _drop_data_carga(df_estoque)
    
    if df_dim_fornecedor is None:
        logger.warning("Dimensão fornecedor é None. Preenchendo fornecedor_id com nulo.")
        df_joined = df_estoque.withColumn("fornecedor_id", F.lit(None).cast("int"))
    else:
        df_dim_fornecedor = _drop_data_carga(df_dim_fornecedor)
        logger.info("Realizando join com a dim_fornecedor para buscar o fornecedor_id...")
        df_joined = df_estoque.join(df_dim_fornecedor, on=["fornecedor"], how="left")
        
    # Remove as colunas descritivas
    logger.info("Removendo colunas descritivas redundantes de produto e fornecedor...")
    cols_to_drop = ["produto", "categoria", "marca", "fornecedor"]
    df_fato = df_joined.drop(*cols_to_drop)
    
    logger.info("Modelagem da fato_estoque concluída.")
    return df_fato
