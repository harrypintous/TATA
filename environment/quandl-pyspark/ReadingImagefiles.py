import os
import sys
import pandas as pd
import numpy as np
import quandl
''' 
os.environ['SPARK_HOME'] = "/opt/spark"
sys.path.append("/home/spark/python")
sys.path.append("/home/spark/python/lib")
sys.path.append("/opt/spark/python/lib/py4j-0.10.8.1-src")
'''
from pyspark.ml import image
from pyspark.shell import sqlContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

import logging
def do_my_logging(log_msg):
    logger = logging.getLogger('__FILE__')
    logger.warning('log_msg = {}'.format(log_msg))

''' 
spark = SparkSession \
    .builder.master("local") \
    .appName("Predicting Fire Dept Calls") \
    .config("spark.executor.memory","16gb")\
    .config("spark.eventLog.enabled=true")\
    .config("spark.eventLog.dir=file:///home/hpinto/log") \
    .getOrCreate()
'''

spark = SparkSession \
    .builder \
    .appName("Python Spark kittens image") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.2.9.jar") \
    .config("spark.master", "local[*]") \
    .getOrCreate()

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
sc = spark.sparkContext

log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.info("pyspark script logger initialized")

spark.sparkContext.setLogLevel('WARN')
spark.sparkContext.setLogLevel('INFO')
spark.sparkContext.setLogLevel('ERROR')

print('Processing, Reading Spark Image files...')
path = '/home/hpinto/PycharmProjects/PYSpark-Learning/quandl-pyspark/kittens'
imageDF = spark.read.format("image") \
    .options(header='false', inferschema='true')\
    .option("dropInvalid", True) \
    .load(path) \
    .toDF("image")


print('Processing, Generate Tempview...')
imageDF.createOrReplaceTempView('imagekitten')

print('Processing, show image records')
spark.sql('select * from imagekitten').show()

print('Processing, read socket records')
# Create DataFrame representing the stream of input lines from connection to localhost:9999;
quandl.ApiConfig.api_key = 'P6LZzSkdVN6zTXQDE6Pd'
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "https://www.quandl.com/api/v3/datatables/WIKI/PRICES/delta.json?api_key=P6LZzSkdVN6zTXQDE6Pd") \
    .option("port", 443) \
    .load()

lines.show()


''' 
# Saving data to a JDBC source
imageDF.write.mode("append") \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "public.imagekitten") \
    .option("user", "postgres") \
    .option("password", "8784") \
    .option("driver", "org.postgresql.Driver") \
    .save()
or
#Specifying create table column data types on write
imageDF.write \
    .option("createTableColumnTypes", "name CHAR(150)") \
    .jdbc("jdbc:postgresql://http:192.168.191.129:5432/postgres", "schema.imagesx",
          properties={"user": "postgres", "password": "8784"}) '''