from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Exemplo").getOrCreate()
    arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
    despachantes = spark.read.csv("/home/william/git/Learning/Fernando_Amaral/download/despachantes.csv", header=False, schema = arqschema)

    calculo = despachantes.select("data").groupBy(year("data")).count()
    calculo.write.format("console").save()
    spark.stop()

    #spark-submit /home/william/git/pyspark/codigos/atividade49.py
