import sys

# Adiciona o diretório base (/opt/airflow) ao sys.path para reconhecer o módulo 'src'
sys.path.append("/opt/airflow")

from src.modules.spark_session import get_spark_session, close_spark_session
from src.modules.minio_utils import create_minio_bucket

def create_and_show_spark_df(): 
    """
    Inicializa ou obtém uma sessão ativa do Spark com suporte ao S3A/MinIO.
    """
    # Inicializa a sessão do Spark usando o módulo centralizado
    spark = get_spark_session("TestSparkApp")
    
    try:
        # Cria o bucket 'test-spark' antes de tentar gravar nele
        create_minio_bucket("test-spark")
        
        data = [("diego", 28), ("rodrigo", 27)]
        columns = ["nome", "idade"]
        df = spark.createDataFrame(data, columns)
        df.show()
        
        # Salva o DataFrame no bucket 'test-spark' do MinIO como Parquet particionado pela coluna 'nome'
        df.write.mode("overwrite").partitionBy("nome").parquet("s3a://test-spark/parquet_data")
        
    finally:
        # Finaliza a sessão do Spark com segurança
        close_spark_session(spark)

if __name__ == "__main__":
    create_and_show_spark_df()