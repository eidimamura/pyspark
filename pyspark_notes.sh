###################################################
#  _______     _______ _____        _____  _  __  #
# |  __ \ \   / / ____|  __ \ /\   |  __ \| |/ /  #
# | |__) \ \_/ / (___ | |__) /  \  | |__) | ' /   #
# |  ___/ \   / \___ \|  ___/ /\ \ |  _  /|  <    #
# | |      | |  ____) | |  / ____ \| | \ \| . \   #
# |_|      |_| |_____/|_| /_/    \_\_|  \_\_|\_\  #
#                                                 #
###################################################



 #                          _ _            
 #                         (_) |           
 #  ___ ___  _ __   ___ ___ _| |_ ___  ___ 
 # / __/ _ \| '_ \ / __/ _ \ | __/ _ \/ __|
 #| (_| (_) | | | | (_|  __/ | || (_) \__ \
 # \___\___/|_| |_|\___\___|_|\__\___/|___/
 #                                         
                                    
#Tabelas gerenciadas
    - Spark gerencia dados e metadados
    - armazenadas no warehouse do spark
    - se excluirmos tudo é apagado, dados e metadados.

#Tabelas não gerenciadas
    - Spark gerencia apenas os metadados
    - Informamos onde as tabelas estão (arquivos .orc por exemplo)
    - se excluirmos apenas os metadados são apagados.
    - é o caso das tabelas que criamos na conta AWS de DEV, via ATHENA

########################################################################################################
#  _                            _   
# (_)                          | |  
#  _ _ __ ___  _ __   ___  _ __| |_ 
# | | '_ ` _ \| '_ \ / _ \| '__| __|
# | | | | | | | |_) | (_) | |  | |_ 
# |_|_| |_| |_| .__/ \___/|_|   \__|
#             | |                   
#             |_|                   
#    
#  
#    _     _ _          
#   (_)   | | |         
#    _  __| | |__   ___ 
#   | |/ _` | '_ \ / __|
#   | | (_| | |_) | (__ 
#   | |\__,_|_.__/ \___|
#  _/ |                 
# |__/                  

#CARREGANDO O PYSPARK COM O JAR DE JDBC
pyspark --jars /home/william/Downloads/postgresql-42.7.4.jar

pyspark --driver-class-path /home/william/Downloads/postgresql-42.7.4.jar

from pyspark.sql import SparkSession

# Crie uma SparkSession
spark = SparkSession.builder \
    .appName("Exemplo JDBC") \
    .config("spark.jars", "/home/william/Downloads/postgresql-42.7.4.jar") \
    .getOrCreate()

# Defina as propriedades de conexão
url = "jdbc:postgresql://localhost:5432/postgres"
properties = {
    "user": "postgres",
    "password": "pwd",
    "driver": "org.postgresql.Driver"
}

# Tente carregar uma tabela
try:
    df = spark.read.jdbc(url=url, table="clientes", properties=properties)
    df.show()  # Mostra as primeiras linhas do DataFrame
    print("Conexão bem-sucedida!")
except Exception as e:
    print("Erro ao conectar:", e)

# Feche a SparkSession
spark.stop()


#importando dados com JDBC
resumo = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "clientes") \
    .option("user", "postgres") \
    .option("password", "pwd") \
    .option("driver", "org.postgresql.Driver") \
    .load()

clientesdata = resumo.select("cliente", "estado")
clientesdata.show();

#salvando os dados via JDBC em uma tabela no postgres
clientesdata.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "clientesdata") \
    .option("user", "postgres") \
    .option("password", "pwd") \
    .option("driver", "org.postgresql.Driver") \
    .save()


###################################################################################################################################
# 
#                        _            
#                       (_)           
#   __ _ _ __ __ _ _   _ ___   _____  
#  / _` | '__/ _` | | | | \ \ / / _ \ 
# | (_| | | | (_| | |_| | |\ V / (_) |
#  \__,_|_|  \__, |\__,_|_| \_/ \___/ 
#               | |                   
#               |_|                   
# 

#importando as funcoes de sql do spark
from pyspark.sql import functions as Func
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#importando arquivo .parquet com o path declarado
clientes = spark.read.format("parquet").load("/home/william/git/Learning/Fernando_Amaral/download/Atividades/Clientes.parquet")
clientes.write.saveAsTable("TB_clientes")
spark.sql("select * from TB_clientes").show()

itensVendas = spark.read.format("parquet").load("/home/william/git/Learning/Fernando_Amaral/download/Atividades/ItensVendas.parquet")
itensVendas.write.saveAsTable("TB_itensVendas")
spark.sql("select * from TB_itensVendas").show()

produtos = spark.read.format("parquet").load("/home/william/git/Learning/Fernando_Amaral/download/Atividades/Produtos.parquet")
produtos.write.saveAsTable("TB_produtos")
spark.sql("select * from TB_produtos").show()

vendas = spark.read.format("parquet").load("/home/william/git/Learning/Fernando_Amaral/download/Atividades/Vendas.parquet")
vendas.write.saveAsTable("TB_vendas")
spark.sql("select * from TB_vendas").show()

vendedores = spark.read.format("parquet").load("/home/william/git/Learning/Fernando_Amaral/download/Atividades/Vendedores.parquet")
vendedores.write.saveAsTable("TB_vendedores")
spark.sql("select * from TB_vendedores").show()

###################################################################################################################################
#
#                                       _ _     
#                                      | | |    
# _ __ ___   ___  _ __   __ _  ___   __| | |__  
#| '_ ` _ \ / _ \| '_ \ / _` |/ _ \ / _` | '_ \ 
#| | | | | | (_) | | | | (_| | (_) | (_| | |_) |
#|_| |_| |_|\___/|_| |_|\__, |\___/ \__,_|_.__/ 
#                        __/ |                  
#                       |___/                   
#
#

#RODAR NA MESMA SESSÃO SHELL QUE O MONGODB FOI INICIADA
#CONECTANDO O PYSPARK COM O MONGODB
pyspark --package org.mongodb.spark:mongo-spark-connector_2.12:3.0.1

#IMPORT DE DADOS DO MONGODB -> DF
df_from_mongodb_data = spark.read.format("mongo") \
       .option("uri","mongodb://127.0.0.1/posts.post") \
       .load()
df_from_mongodb_data.show()

#EXPORT DE DADOS DO DF -> MONGODB
df_from_mongodb_data.write.format("mongo") \
       .option("uri","mongodb://127.0.0.1/posts2.post") \
       .save()



########################################################################################################
#
#           _____ _____     _______     _______ _____        _____  _  __
#     /\   |  __ \_   _|   |  __ \ \   / / ____|  __ \ /\   |  __ \| |/ /
#    /  \  | |__) || |     | |__) \ \_/ / (___ | |__) /  \  | |__) | ' / 
#   / /\ \ |  ___/ | |     |  ___/ \   / \___ \|  ___/ /\ \ |  _  /|  <  
#  / ____ \| |    _| |_    | |      | |  ____) | |  / ____ \| | \ \| . \ 
# /_/    \_\_|   |_____|   |_|      |_| |_____/|_| /_/    \_\_|  \_\_|\_\
#                                                                        
#                                                                        
# 
#      _       _           __                          
#     | |     | |         / _|                         
#   __| | __ _| |_ __ _  | |_ _ __ __ _ _ __ ___   ___ 
#  / _` |/ _` | __/ _` | |  _| '__/ _` | '_ ` _ \ / _ \
# | (_| | (_| | || (_| | | | | | | (_| | | | | | |  __/
#  \__,_|\__,_|\__\__,_| |_| |_|  \__,_|_| |_| |_|\___|
#                                                      
#                                                      

#DATAFRAMES
    #CREATE
    df_clientes = spark.sql("select * from tb_clientes")
    df_itensvendas = spark.sql("select * from tb_itensvendas")
    df_produtos = spark.sql("select * from tb_produtos")
    df_vendas = spark.sql("select * from tb_vendas")
    df_vendedores = spark.sql("select * from tb_vendedores")
    
    #SHOW
    df_clientes.show()
    df_itensvendas.show()
    df_produtos.show()
    df_vendas.show()
    df_vendedores.show()

    despachantes.select("nome","venda").show()
    despachantes.select("nome","venda").where(Func.col("venda") > 20 ).show()
    
        #Query do df clientes, mostrando as colunas Cliente, Estado e Nome, ordenado pelo nome do cliente.
        clientes.select("Cliente", "Estado", "Status").orderBy(Func.col("Cliente")).show(clientes.count(), truncate=False)

        #Query do df clientes, com where para status Gold e Platinum
        clientes.select("Cliente", "Status").where((Func.col("Status")=="Platinum") | (Func.col("Status")=="Gold")).orderBy(Func.col("Status")).show(clientes.count(), truncate=False)

        #QUERY COM SUM E RENAME DE COLUNA
        venda_por_ClienteID = vendas.groupBy("ClienteID") \
            .sum("Total") \
            .withColumnRenamed("sum(Total)", "TotalVendas") \
            .orderBy(Func.col("ClienteID").asc()) 
        #venda_por_ClienteID.show(venda_por_ClienteID.count(), truncate=False)

        venda_por_status = venda_por_Cliente_com_nome.groupBy("Status") \
            .sum("TotalVendas") \
            .withColumnRenamed("sum(TotalVendas)", "TotalPorStatus") \
            .orderBy(Func.col("TotalPorStatus").asc())
        #venda_por_status.show()


    # Exibindo todas as linhas do DataFrame
    clientes.show(clientes.count(), truncate=False)
    #query de select *
    clientes.select("*").show()
    #consulta de select com spark - com where 
    clientes.select("ClienteID","Cliente").where(Func.col("ClienteID") > 20).show()
    #consulta de select com spark - com 20 linhas padrão
    clientes.select("Cliente", "Estado", "Status").show()
    #consulta de select com spark - com 100 linhas
    clientes.select("Cliente", "Estado", "Status").show(100)
    #query de select *
    clientes.select("*").show()
    #consulta de select com spark - com where 
    clientes.select("ClienteID","Cliente").where(Func.col("ClienteID") > 20).show()
    #consulta de select com spark - com 100 linhas
    clientes.select("Cliente", "Estado", "Status").show(100)
    #consulta com agregação e contagem, além de ordenação
    clientes.groupBy("Status").count().orderBy(Func.col("count").desc()).show()
    #Agrupamento de vendas por cliente, sem truncar valor e ordenado pela coluna ClienteID
    vendas.groupBy("ClienteID").sum("Total").orderBy(Func.col("ClienteID").desc()).show(1000, truncate=False)



    #SCHEMA
    #mostrando o schema do df
    clientes.schema
    itensVendas.schema
    produtos.schema
    vendas.schema
    vendedores.schema

    #IMPORT CSV as DF
    recschema = "idrec INT, datarec STRING, iddesp INT"
    reclamacoes = spark.read.csv("/home/william/git/Learning/Fernando_Amaral/download/reclamacoes.csv", header=False, schema = recschema)
    reclamacoes.write.saveAsTable("reclamacoes") #saving as table

    #JOIN COM PYSPARK
        #INNER
        despachantes.join(reclamacoes, despachantes.id == reclamacoes.iddesp, "inner").select("idrec", "datarec", "iddesp","nome").show()
        #RIGHT
        despachantes.join(reclamacoes, despachantes.id == reclamacoes.iddesp, "right").select("idrec", "datarec", "iddesp","nome").show()
        #LEFT
        despachantes.join(reclamacoes, despachantes.id == reclamacoes.iddesp, "left").select("idrec", "datarec", "iddesp","nome").show()
        
        #EXEMPLOS
        venda_por_Cliente_com_nome = clientes.join(venda_por_ClienteID, clientes.ClienteID == venda_por_ClienteID.ClienteID, "Inner") \
                                             .drop(venda_por_ClienteID.ClienteID)
        
        #criando dataframe intermediário de vendas por status com join
        vendas_por_status = venda_por_ClienteID.join(clientes, clientes.ClienteID as ignore_ClienteID == venda_por_ClienteID.ClienteID, "inner")
        vendas_por_cliente = vendas.join(clientes, clientes.ClienteID == vendas.ClienteID)
        vendas_por_cliente.groupBy("Status").sum("Total").show()

        #JOIN MULTIPLO DE VÁRIAS TABELAS
        df_balanco = df_clientes.join(df_vendas, df_clientes.ClienteID == df_vendas.ClienteID, "inner") \
                                .join(df_itensvendas, df_itensvendas.VendasID == df_vendas.VendasID, "inner") \
                                .join(df_produtos, df_produtos.ProdutoID == df_itensvendas.ProdutoID, "inner") \
                                .join(df_vendedores, df_vendedores.VendedorID == df_vendas.VendedorID, "inner") \
                                .drop(df_vendas.ClienteID) \
                                .drop(df_itensvendas.VendasID) \
                                .drop(df_itensvendas.ProdutoID) \
                                .drop(df_vendas.VendedorID)
        df_balanco.count()                        






df_join = clientes.join(vendas, clientes.ClienteID == vendas.ClienteID, "inner") \
                  .join(itensVendas, vendas.VendasID == itensVendas.VendasID, "inner") \
                  .join(produtos, itensVendas.ProdutoID == produtos.ProdutoID, "inner")

# Selecionando as colunas desejadas
df_resultado = df_join.select("clientes.Cliente", "vendas.Data", "produtos.Produto", "itensVendas.ValorTotal")

# Exibindo o resultado



########################################################################################################
#
#   _____ _____        _____  _  __   _____  ____  _      
#  / ____|  __ \ /\   |  __ \| |/ /  / ____|/ __ \| |     
# | (___ | |__) /  \  | |__) | ' /  | (___ | |  | | |     
#  \___ \|  ___/ /\ \ |  _  /|  <    \___ \| |  | | |     
#  ____) | |  / ____ \| | \ \| . \   ____) | |__| | |____ 
# |_____/|_| /_/    \_\_|  \_\_|\_\ |_____/ \___\_\______|
#                                                         
#                                                         



#SPARK SQL

#DATABASES
    #SHOW
    spark.sql("show databases").show()

    #CREATE
    spark.sql("create database desp")
    spark.sql("create database DB_loja")

    #USE
    spark.sql("use desp").show()
    spark.sql("use DB_loja").show()

#DATAFRAMES
    #SELECT
    spark.sql("select * from despachantes").show()

#TABLES
    #SHOW
    spark.sql("show tables").show()
    #CATALOGO
    spark.catalog.listTables()
    #OBS - outra forma de ver se a tabela é gerencia(externa) ou não

    #CREATE - GERENCIADA
    arqschema = "id INT, nome STRING, status STRING, cidade STRING, venda INT, dado STRING" #schema
    despachantes = spark.read.csv("/home/william/git/Learning/Fernando_Amaral/download/despachantes.csv", header=False, schema=arqschema) #import para o df
    despachantes.write.saveAsTable("Despachantes")

    #CREATE - GERENCIADA COM OVERWRITE
    despachantes.write.mode("overwrite").saveAsTable("Despachantes")

    #CREATE - NÃO GERENCIADA salvando uma tabela não gerenciada (externa)
    despachantes.write.mode("overwrite").format("parquet").save("/home/william/git/Pyspark/tables/despachantes.parquet")
    despachantes.write.option("path","/home/william/git/Pyspark/tables/despachantes_ng").saveAsTable("Despachantes_ng")

    #SELECT
    spark.sql("select * from despachantes").show()
    spark.sql("select reclamacoes.*, despachantes.nome from despachantes inner join reclamacoes on(despachantes.id = reclamacoes.iddesp)").show()
    spark.sql("select reclamacoes.*, despachantes.nome from despachantes right join reclamacoes on(despachantes.id = reclamacoes.iddesp)").show()
    spark.sql("select reclamacoes.*, despachantes.nome from despachantes left join reclamacoes on(despachantes.id = reclamacoes.iddesp)").show()

    #JOIN COM SPARK SQL
        #INNER
        spark.sql("select reclamacoes.*, despachantes.nome from despachantes inner join reclamacoes on(despachantes.id = reclamacoes.iddesp)").show()
        #RIGHT
        spark.sql("select reclamacoes.*, despachantes.nome from despachantes right join reclamacoes on(despachantes.id = reclamacoes.iddesp)").show()
        #LEFT
        spark.sql("select reclamacoes.*, despachantes.nome from despachantes left join reclamacoes on(despachantes.id = reclamacoes.iddesp)").show()

    spark.sql(
    "select tb_clientes.Cliente, tb_vendas.Data, tb_produtos.Produto, tb_vendedores.Vendedor, tb_itensvendas.ValorTotal "
        "from tb_clientes "
            "inner join tb_vendas on (tb_vendas.ClienteID = tb_clientes.ClienteID) "
            "inner join tb_itensvendas on (tb_itensvendas.VendasID = tb_vendas.VendasID) "
            "inner join tb_produtos on (tb_produtos.ProdutoID = tb_itensvendas.ProdutoID) "
            "inner join tb_vendedores on (tb_vendedores.VendedorID = tb_vendas.VendedorID) "
        "order by tb_clientes.ClienteID "
    ).show() 

#VIEWS
    #SHOW VIEWS
        spark.sql("show views").show()

    #CREATE VIEW TEMPORÁRIA
    despachantes.createOrReplaceTempView("Despachantes_view1")

    #CREATE VIEW TEMPORÁRIA
    despachantes.createOrReplaceGlobalTempView("Despachantes_view2")

    #CREATE VIEW VIA SELECT
    spark.sql("create or replace temp view desp_view as select * from despachantes")

    #SELECT - VIEW TEMPORÁRIA
    spark.sql("select * from Despachantes_view1").show()
    spark.sql("select * from desp_view").show()

    #SELECT - VIEW GLOBAL
    spark.sql("select * from global_temp.Despachantes_view2").show()

    #DROP
    spark.sql("drop view global_temp.Despachantes_view2")
    spark.sql("drop view Despachantes_view1")
    spark.sql("drop view desp_view")

#DDL
    #GERANDO O CREATE TABLE
    spark.sql("show create table despachantes_ng").show(truncate=False)
    spark.sql("show create table despachantes").show(truncate=False)
    #OBS - Se aparecer a location da tabela, é uma tabela externa(não gerenciável), caso contrário é uma tabela interna


#INFO
    #DIRETORIO ONDE FICAM AS TABELAS GERENCIADAS EM UMA INSTALAÇÃO DE SPARK
    /home/william/spark-warehouse/



#IMPORT

    #gerando o schema de um df
    arqschema = "id INT, nome STRING, status STRING, cidade STRING, venda INT, dado STRING"

    #CSV IMPORT
    #importando um dataframe sem header, através de um csv
    despachantes = spark.read.csv("/home/william/git/Learning/Fernando_Amaral/download/despachantes.csv", header=False, schema=arqschema)
    despachantes.show()
    despachantes.schema

###########################
 #   ____  ____   _____   #
 #  / __ \|  _ \ / ____|  #
 # | |  | | |_) | (___    #
 # | |  | |  _ < \___ \   #
 # | |__| | |_) |____) |  #
 #  \____/|____/|_____/   #
 #                        #
###########################  
                    
 - executar o spark-sql no console
 - e usar o SQL normalmente nas tabelas e banco.



############################################################################### 
#        _______ _______      _______ _____          _____  ______  _____     # 
#     /\|__   __|_   _\ \    / /_   _|  __ \   /\   |  __ \|  ____|/ ____|    #
#    /  \  | |    | |  \ \  / /  | | | |  | | /  \  | |  | | |__  | (___      #
#   / /\ \ | |    | |   \ \/ /   | | | |  | |/ /\ \ | |  | |  __|  \___ \     #
#  / ____ \| |   _| |_   \  /   _| |_| |__| / ____ \| |__| | |____ ____) |    #
# /_/    \_\_|  |_____|   \/   |_____|_____/_/    \_\_____/|______|_____/     #
#                                                                             #
###############################################################################              


 #QUERIES DAS ATIVIDADES
#Atividade 28.1)Query do df clientes, mostrando as colunas Cliente, Estado e Nome, ordenado pelo nome do cliente.
clientes.select("Cliente", "Estado", "Status").orderBy(Func.col("Cliente")).show(clientes.count(), truncate=False)

#Atividade 28.2)Query do df clientes, com where para status Gold e Platinum
clientes.select("Cliente", "Status").where((Func.col("Status")=="Platinum") | (Func.col("Status")=="Gold")).orderBy(Func.col("Status")).show(clientes.count(), truncate=False)

#Atividade 28.3) Query com Joins
venda_por_ClienteID = vendas.groupBy("ClienteID") \
        .sum("Total") \
        .withColumnRenamed("sum(Total)", "TotalVendas") \
        .orderBy(Func.col("ClienteID").asc()) 
venda_por_ClienteID.show(venda_por_ClienteID.count(), truncate=False)

venda_por_Cliente_com_nome = clientes.join(venda_por_ClienteID, clientes.ClienteID == venda_por_ClienteID.ClienteID, "Inner") \
        .drop(venda_por_ClienteID.ClienteID)
venda_por_Cliente_com_nome.show()

venda_por_status = venda_por_Cliente_com_nome.groupBy("Status") \
        .sum("TotalVendas") \
        .withColumnRenamed("sum(TotalVendas)", "TotalPorStatus") \
        .orderBy(Func.col("TotalPorStatus").asc())
venda_por_status.show()

#Atividade 38.2) Query com Joins - mostrar item vendido, nome do cliente, data da venda, produto, vendedor e valor total dos itens.
df_nota_fiscal = spark.sql("select")

select Cliente, Data, Produto, Vendedor, ValorTotal from tb_clientes \ 
        inner join on (tb_clientes.ClienteID == tb_vendas.ClienteID) \
        inner join on (tb_vendas.VendasID == tb_itensvendas.VendaID)

spark.sql(
    "select tb_clientes.Cliente, tb_vendas.Data, tb_produtos.Produto, tb_vendedores.Vendedor, tb_itensvendas.ValorTotal "
        "from tb_clientes "
            "inner join tb_vendas on (tb_vendas.ClienteID = tb_clientes.ClienteID) "
            "inner join tb_itensvendas on (tb_itensvendas.VendasID = tb_vendas.VendasID) "
            "inner join tb_produtos on (tb_produtos.ProdutoID = tb_itensvendas.ProdutoID) "
            "inner join tb_vendedores on (tb_vendedores.VendedorID = tb_vendas.VendedorID) "
        "order by tb_clientes.ClienteID "
).show() 

#Atividade 49 - Criando uma aplicação em pyspark
#RODAR O ARQUIVO atividade49.py via spark-submit
#spark-submit atividade49.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Exemplo").getOrCreate()
    arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
    despachantes = spark.read.csv("/home/william/git/Learning/Fernando_Amaral/download/despachantes.csv", header=False, schema = arqschema)

    calculo = despachantes.select("data").groupBy(year("data")).count()
    calculo.write.format("console").save()
    spark.stop()
