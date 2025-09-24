from pyspark.sql import SparkSession

def create_spark_session(app_name="SparkMVCApp", master="local[*]"):
    """
    Crea y devuelve una SparkSession.
    :param app_name: Nombre de la aplicación Spark
    :param master: Configuración del cluster (ej. local[*] o spark://...)
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    return spark
