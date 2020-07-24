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
'''
getdata = quandl.get('FRED/GDP', start_date='2010-01-01', end_date='2014-01-01',
                     collapse='annual', transformation='rdiff',
                     rows=4)'''
data = quandl.get_table('MER/F1', compnumber="39102", paginate=True)
print('get data', data)
data.head()


'''
dfClass = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "class") \
    .option("user", "postgres") \
    .option("password", "8784") \
    .option("driver", "org.postgresql.Driver") \
    .load()

dfPerson = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "person") \
    .option("user", "postgres") \
    .option("password", "8784") \
    .option("driver", "org.postgresql.Driver") \
    .load()

dfClass.createOrReplaceTempView("dclass")
dfPerson.createOrReplaceTempView("dperson")
#spark.sql("select * from dclass limit 10").show()
#spark.sql("select * from dperson limit 10").show()

dfPerson.join(dfClass, "name").show(10)

spark.sql("select * from dclass d1 inner join dperson d2 on d1.name = d2.name").show()  '''