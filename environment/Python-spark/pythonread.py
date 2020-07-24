# PathreduceByKey

import os
import sys
from functools import reduce
from pyspark import SparkContext, SparkConf
from pyspark.shell import sc

os.environ['SPARK_HOME'] = "/opt/spark"
sys.path.append("/home/spark/python")
sys.path.append("/home/spark/python/lib")
sys.path.append("/opt/spark/python/lib/py4j-0.10.8.1-src")

path = "/home/hpinto/Desktop/mySpark-data/DimCurrency.csv"
read_file = open(path,'r')
days = read_file.read()
print(days)

pathw = "/home/hpinto/Desktop/mySpark-data/DimnewCurrency.csv"
wr_file = open(pathw,'a+')
wr = wr_file.write(days)
print(wr)

#Exception: Python in worker has different version 2.7 than that in driver 3.7, PySpark cannot run with different minor versions.Please check environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are correctly set.

pairs = map(lambda s: (s, 1),days)
counts = reduce(lambda a, b: a + b,days)
print(counts)

lines = sc.textFile("/home/hpinto/Desktop/mySpark-data/DimAccounts.csv")              # Distribute the data - Create a RDD

countX = (lines.flatMap(lambda x: x.split(' '))          # Create a list with all words
                  .map(lambda x: (x, 1))                 # Create tuple (word,1)
                  .reduceByKey(lambda x,y : x + y)
                           )      # reduce by key i.e. the word
#output = countX.take(100)                                 # get the output on local
x1 = countX.take(100)

for (word, count) in x1:                             # print output
    print("%s: %i" % (word, count))