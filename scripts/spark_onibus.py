from pyspark import SparkContext


sc = SparkContext("local","onibus")
sqlc = SQLContext(sc)

linhasRDD = sqlc.read.json("hdfs://imacedo:9000/user/engdados/input/2018_04_21_linhas.json").rdd

tabelaLinhaRDD = sqlc.read.json("hdfs://imacedo:9000/user/engdados/input/2018_04_21_tabelaLinhas.json").rdd

trechosItinerariosRDD = sqlc.read.json("hdfs://imacedo:9000/user/input/2018_04_21_trechosItinerarios.json").rdd



#dataInicial = sc.textFile("hdfs://imacedo:9000/user/engdados/input/conjunto2.csv")

#linhaSeparada = dataInicial.map(lambda line: line.split(";"))
#nomeEemprego = linhaSeparada.map(lambda lista: (lista[0], lista[1]))
#nomesUnicos = nomeEemprego.distinct()
#empregoEchave = nomesUnicos.map(lambda e: (e[1], 1))
#empregoContado = empregoEchave.reduceByKey(lambda acc, curr: acc + curr)

#empregoContado.saveAsTextFile("hdfs://imacedo:9000/user/engdados/output/spark/empregoContado.txt")
