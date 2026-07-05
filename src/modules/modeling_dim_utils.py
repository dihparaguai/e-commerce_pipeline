from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from loguru import logger

def create_dim_produto(df_vendas: DataFrame, df_estoque: DataFrame, df_devolucoes: DataFrame) -> DataFrame:
    """
    Cria a tabela dim_produto a partir do union dos dados disponíveis de vendas, estoque e devoluções.
    Ignora fontes que forem passadas como None.
    Agrupa por produto_id e prioriza registros com valores não nulos.
    """
    logger.info("Iniciando a modelagem da dimensão produto (dim_produto)...")
    
    # Colunas requeridas para a dimensão produto
    dim_cols = ["produto_id", "produto", "categoria", "marca"]
    parts = []
    
    # Processa vendas se não for None
    if df_vendas is not None:
        logger.debug("Selecionando colunas de produto existentes em Vendas...")
        v_cols = [c for c in dim_cols if c in df_vendas.columns]
        parts.append(df_vendas.select(*v_cols))
        
    # Processa estoque se não for None
    if df_estoque is not None:
        logger.debug("Selecionando colunas de produto existentes em Estoque...")
        e_cols = [c for c in dim_cols if c in df_estoque.columns]
        parts.append(df_estoque.select(*e_cols))
        
    # Processa devolucoes se não for None
    if df_devolucoes is not None:
        logger.debug("Selecionando colunas de produto existentes em Devoluções...")
        d_cols = [c for c in dim_cols if c in df_devolucoes.columns]
        parts.append(df_devolucoes.select(*d_cols))
        
    if not parts:
        logger.warning("Nenhuma tabela de origem Silver válida foi fornecida.")
        return None
        
    # Realiza o unionByName permitindo colunas ausentes
    logger.info("Realizando union das origens de produtos disponíveis...")
    df_union = parts[0]
    for df_part in parts[1:]:
        df_union = df_union.unionByName(df_part, allowMissingColumns=True)
        
    # Deduplica agrupando por produto_id e pegando a primeira ocorrência não nula (ignorenulls=True)
    logger.info("Deduplicando registros de produto_id e mesclando informações não nulas...")
    df_dedup = (
        df_union
        .groupBy("produto_id")
        .agg(
            F.first("produto", ignorenulls=True).alias("produto"),
            F.first("categoria", ignorenulls=True).alias("categoria"),
            F.first("marca", ignorenulls=True).alias("marca")
        )
    )
    
    logger.info("Modelagem da dim_produto concluída.")
    return df_dedup
