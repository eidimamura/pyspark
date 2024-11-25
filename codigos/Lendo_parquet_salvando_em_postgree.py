import sys, getopt  # Import para manipulação de argumentos da linha de comando
from pyspark.sql import SparkSession  

#24/11/2024 - William Imamura
#Lê um parquet no sistema e escreve o mesmo em uma tabela postgree

# Exemplos de como executar o script:
# spark-submit --jars /home/william/Downloads/postgresql-42.7.4.jar /home/william/git/pyspark/codigos/Lendo_parquet_salvando_em_postgree.py -a /home/william/git/Learning/Fernando_Amaral/download/Atividades/Clientes.parquet -t tb_clientes
  
# Verifica se o script está sendo executado diretamente
if __name__ == "__main__":  
    # Inicializa uma sessão Spark com o nome "parquet_to_sql_table"
    spark = SparkSession.builder.appName("parquet_to_sql_table").getOrCreate() 
    
    # Captura os argumentos da linha de comando e define as opções esperadas (-t(formato), -i(arquivo de entrada))
    opts, args = getopt.getopt(sys.argv[1:], "a:t:")  
    
    #Inicialização
    arquivo_entrada, tabela = "", "" 
    
    for opt, arg in opts: 
        if opt == "-a":
            arquivo_entrada = arg
        elif opt == "-t":
            tabela = arg
        
    # Lê os dados do arquivo CSV especificado
    dados = spark.read.load(arquivo_entrada)
    dados.write.format("console").save() #printa o DF no console

    # Escrita do DF na tabela postgres, caso exista, a mesma será sobreescrita
    dados.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/will_bank") \
        .option("dbtable", tabela) \
        .option("user", "postgres") \
        .option("password", "pwd") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    spark.stop()  # Para a sessão Spark

    