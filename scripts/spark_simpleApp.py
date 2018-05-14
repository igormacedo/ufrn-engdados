"""SimpleApp.py""" 
from pyspark import SparkContext
 
sc = SparkContext("local", "SimpleApp") 
logData = sc.textFile("hdfs://imacedo:9000/user/engdados/input/input.txt") 

numAs = logData.filter(lambda s: 'c' in s).count() 
numBs = logData.filter(lambda s: 'd' in s).count() 

print "\n\nLines with c: %i, lines with d: %i\n" % (numAs, numBs)
