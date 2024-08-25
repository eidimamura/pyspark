#importando as funcoes de sql do spark
from pyspark.sql import functions as Func

#importando arquivo .parquet com o path declarado
clientes = spark.read.format("parquet").load("/home/william/pyspak/download/Atividades/Clientes.parquet")

itensVendas = spark.read.format("parquet").load("/home/william/pyspak/download/Atividades/ItensVendas.parquet")

produtos = spark.read.format("parquet").load("/home/william/pyspak/download/Atividades/Produtos.parquet")

vendas = spark.read.format("parquet").load("/home/william/pyspak/download/Atividades/Vendas.parquet")

vendedores = spark.read.format("parquet").load("/home/william/pyspak/download/Atividades/Vendedores.parquet")


#dando show no df
clientes.show()
itensVendas.show()
produtos.show()
vendas.show()
vendedores.show()


#mostrando o schema do df
clientes.schema
itensVendas.schema
produtos.schema
vendas.schema
vendedores.schema

#consulta de select com spark - com where 
clientes.select("ClienteID","Cliente").where(Func.col("ClienteID") > 20).show()
#consulta de select com spark - com 20 linhas padrão
clientes.select("Cliente", "Estado", "Status").show()
#consulta de select com spark - com 100 linhas
clientes.select("Cliente", "Estado", "Status").show(100)

#consulta de select com spark - com or where
clientes.select("Cliente", "Estado", "Status").where((Func.col("Status")=="Platinum") | (Func.col("Status")=="Gold" )).show()

#consulta com agregação e contagem, além de ordenação
clientes.groupBy("Status").count().orderBy(Func.col("count").desc()).show()

#Agrupamento de vendas por cliente, sem truncar valor e ordenado pela coluna ClienteID
vendas.groupBy("ClienteID").sum("Total").orderBy(Func.col("ClienteID").desc()).show(1000, truncate=False)

#criando dataframe de vendas por cliente
venda_por_ClienteID = vendas.groupBy("ClienteID").sum("Total").orderBy(Func.col("ClienteID").desc())

#criando dataframe intermediário de vendas por status com join
vendas_por_status = venda_por_ClienteID.join(clientes, clientes.ClienteID as ignore_ClienteID == venda_por_ClienteID.ClienteID, "inner")


vendas_por_cliente = vendas.join(clientes, clientes.ClienteID == vendas.ClienteID)

vendas_por_cliente.groupBy("Status").sum("Total").show()
