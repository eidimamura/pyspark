# Importa os módulos sys e getopt para manipulação de argumentos da linha de comando
import sys, getopt  
# Importa a classe SparkSession do PySpark
from pyspark.sql import SparkSession  

# Verifica se o script está sendo executado diretamente
if __name__ == "__main__":  
    # Inicializa uma sessão Spark com o nome "parquet_to_sql_table"
    spark = SparkSession.builder.appName("parquet_to_sql_table").getOrCreate() 
    
    # Captura os argumentos da linha de comando e define as opções esperadas (-t(formato), -i(arquivo de entrada))
    opts, args = getopt.getopt(sys.argv[1:], "a:t:")  
    
    # Inicializa as variáveis para formato, arquivo de entrada e diretório de saída
    arquivo, tabela = "", "" 
    
    for opt, arg in opts: 
        if opt == "-a":
            arquivo = arg
        elif opt == "-t":
            tabela = arg
        
    # Lê os dados do arquivo CSV especificado
    dados = spark.read.load(arquivo)
    dados.write.format("console").save()

    dados.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/will_bank") \
        .option("dbtable", tabela) \
        .option("user", "postgres") \
        .option("password", "pwd") \
        .option("driver", "org.postgresql.Driver") \
        .save()


    spark.stop()  # Para a sessão Spark

    # Exemplos de como executar o script:
    # spark-submit --jars /home/william/Downloads/postgresql-42.7.4.jar /home/william/git/pyspark/codigos/Lendo_parquet_salvando_em_postgree.py -a /home/william/git/Learning/Fernando_Amaral/download/Atividades/Clientes.parquet -t XIXI
    