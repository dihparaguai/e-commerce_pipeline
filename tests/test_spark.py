from pyspark.sql import SparkSession

def create_and_show_spark_df(): 
    # Inicializa ou obtém uma sessão ativa do Spark.
    # O appName define o nome da aplicação que aparecerá no Spark Web UI.
    spark = (
        SparkSession.builder
        .appName("TestSparkApp")
        .getOrCreate()
    )
    
    data = [("diego", 28), ("rodrigo", 27)]
    columns = ["nome", "idade"]
    df = spark.createDataFrame(data, columns)
    df.show()
    
    spark.stop()


if __name__ == "__main__":
    create_and_show_spark_df()