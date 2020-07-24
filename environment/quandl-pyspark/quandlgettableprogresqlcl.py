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



#--postgresql open database & Read TradeData Table
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "public.TradeData") \
    .option("user", "postgres") \
    .option("password", "8784") \
    .option("driver", "org.postgresql.Driver") \
    .load()

if __name__ == '__main__':
    # Enable Arrow-based columnar data transfers
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    dfcol =df.toPandas()
    pdDF = pd.DataFrame(dfcol)


    print('Calculating part A')
    pdDF['% chg Close'] =  (pdDF['open'] / pdDF['close'].shift(1) - 1).fillna(0)
    pdDF['Alt CloseToOpen'] = pdDF['open'].sub(pdDF['close'].shift()).div(pdDF['close'] - 1).fillna(0)
    pdDF['CloseToOpen cumsum'] = pdDF['Alt CloseToOpen'].cumsum()
    print(pdDF.head(10))
    
    # Create a Spark DataFrame from a pandas DataFrame using Arrow base
    sparkDF = spark.createDataFrame(pdDF)

    sparkDF.createOrReplaceTempView('dailytrade')
    # SQL Execute records from tables
    spark.sql('select * from dailytrade order by 1,2').show(5)

    print('Calculating part B')
    spark.sql("show databases").show()

spark.sql("use postgres")
spark.sql("show databases").show()







'''
index_values= (pd.date_range(pdDF['date'],
                                  periods=30, freq='D'))
    # Creating a date_time form index
    pdDF['date'] = (pd.date_range(pdDF['date'],
                                  periods=30, freq='D'))

    # Creating a series using 'index_values' 
    # Notice, one of the series value is nan value 
    pdDF['series'] = (pd.Series([pdDF['close']],
                        index= pdDF['date']))

    # Creating dataframe using the series 
    pdDF['asFreq']  = pd.DataFrame({"asfreq": pdDF['series']})

    # Print the Dataframe 
    print(pdDF.head())


 '''











