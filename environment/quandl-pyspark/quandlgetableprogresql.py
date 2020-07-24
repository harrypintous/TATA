#import sparkenv
# -----------------------python basic imports
import quandl
import os
from os.path import expanduser, join, abspath
# import pandas_datareader.data as web
# import Data_Api
# import time
# import sparkenv
# import json

# ---------------------sparks imports
# import sys
from pyspark.shell import sqlContext, sc
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

# ----------------Import pandas and sqlite3
import pandas as pd
import numpy as np
import sqlite3
from sqlite3 import Error

from pyspark.sql import SQLContext

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.2.9.jar") \
    .config("spark.master", "local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')
spark.sparkContext.setLogLevel('INFO')
spark.sparkContext.setLogLevel('ERROR')

quandl.ApiConfig.api_key = 'P6LZzSkdVN6zTXQDE6Pd'

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

#data = quandl.get_table('WIKI/PRICES', paginate=True)
qGET = quandl.get_table('WIKI/PRICES', qopts = { 'columns': [] }, ticker = [], date = { 'gte': '2018-01-01', 'lte': '2018-12-31' }, paginate=True)
print(qGET)
# Generate a pandas DataFrame
pdDF = pd.DataFrame(qGET)

# Create a Spark DataFrame from a pandas DataFrame using Arrow base
sparkDF = spark.createDataFrame(pdDF)

# Rename the Column
dataDF = (sparkDF.withColumnRenamed("Total Trade Quantity", "TotalTradeQuantity")
                 .withColumnRenamed("Turnover (Lacs)","TurnoverLacs"))

# Create a Spark DataFrame Create Temp view
dataDF.createOrReplaceTempView('wikitrade')

insertDF = spark.sql("select * from wikitrade")
insertDF.show()

''' 
# Saving data to a JDBC source
dataDF.write.mode("append") \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "public.TradeDatabk") \
    .option("user", "postgres") \
    .option("password", "8784") \
    .option("driver", "org.postgresql.Driver") \
    .save()
'''
