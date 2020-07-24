from pyspark import SparkContext, SparkConf
from os.path import expanduser, join, abspath
from pyspark.shell import sqlContext
from pyspark.sql import SparkSession


def hive(n):    # write Fibonacci series up to n
    result = []
    spark = SparkSession \
        .builder.master("local") \
        .appName("Predicting Fire Dept Calls") \
        .config("spark.executor.memory", "6gb") \
        .config("spark.sql.warehouse.dir", 'jdbc:hive2://localhost:10000') \
        .enableHiveSupport() \
        .getOrCreate()

    return spark


def fib2(n):   # return Fibonacci series up to n
    result = []
    a, b = 0, 1
    while b < n:
        result.append(b)
        a, b = b, a+b
    return result


