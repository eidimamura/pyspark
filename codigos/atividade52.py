import sys, getopt
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Exemplo").getOrCreate()
    opts, args = getopt.getopt(sys.argv[1:], "t:i:o:")
    formato, infile, outdir = "","",""

    for opt, arg in opts:
            if opt == "-t":
                  formato = arg
            elif opt == "-i":
                  infile = arg
            elif opt == "-o":
                  outdir = arg

    dados = spark.read.csv(infile, header=False, inferSchema = True)
    dados.write.mode("overwrite").format(formato).save(outdir)

    spark.stop()

    #spark-submit /home/william/git/pyspark/codigos/atividade52.py -t parquet -i /home/william/git/Learning/Fernando_Amaral/download/despachantes.csv -o /home/william/git/pyspark/output/
    #spark-submit /home/william/git/pyspark/codigos/atividade52.py -t json -i /home/william/git/Learning/Fernando_Amaral/download/despachantes.csv -o /home/william/git/pyspark/output/
    #spark-submit /home/william/git/pyspark/codigos/atividade52.py -t orc -i /home/william/git/Learning/Fernando_Amaral/download/despachantes.csv -o /home/william/git/pyspark/output/
    