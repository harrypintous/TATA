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
print(sc)
#spark = SparkSession.builder.appName('pandasToSparkDF').getOrCreate()

warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# Open Quandl section store in pandas
quandl.ApiConfig.api_key = 'P6LZzSkdVN6zTXQDE6Pd'
qGET = quandl.Dataset('NSE/OIL').data()
toPD = qGET.to_pandas()


# Generate a pandas DataFrame
pdDF = pd.DataFrame(toPD)

# Create a Spark DataFrame from a pandas DataFrame using Arrow
sparkDF = spark.createDataFrame(pdDF)
#print('sparkDF')
#sparkDF.show()

dataDF = (sparkDF.withColumnRenamed("Total Trade Quantity", "TotalTradeQuantity")
                 .withColumnRenamed("Turnover (Lacs)","TurnoverLacs"))

#Create a Spark DataFrame using SQL
dataDF.createOrReplaceTempView("DimNSEOIL")
#print('SQLsparkDF')
#spark.sql("select * from DimNSEOIL").show()

#sparkenv.hive(spark)
'''
spark.sql("show databases").show()
spark.sql("use adventurework").show()
spark.sql("show tables").show() '''


spark.sql("use default").show()
spark.sql("show tables").show()


#spark.sql("show databases").show()
#spark.sql("show tables").show()
insertDF = spark.sql("select * from DimNSEOIL")
# spark.sql("select * from DimAppl").show()




'''
sparkDF.write.format("orc").mode("append").saveAsTable("DimAppl")
spark.sql("show tables").show()
insertDF = spark.sql("select * from DimAppl")

'''
print(spark.sql("select * from DimAppl").count())

print('insertDF')
insertDF.write.mode("append").saveAsTable("DimAppl")
spark.sql("select * from DimAppl").show()
print(spark.sql("select * from DimAppl").count())