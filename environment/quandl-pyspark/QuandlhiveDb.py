#import sparkenv
# -----------------------python basic imports
import os
import sys
import pandas as pd
import numpy as np
import quandl
import sqlite3
from sqlite3 import Error

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

from pyspark.sql import SQLContext
print(sc)
#spark = SparkSession.builder.appName('pandasToSparkDF').getOrCreate()

warehouse_location = abspath('spark-warehouse')
#- hive database
spark = SparkSession \
    .builder.master("local") \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# Streaming Quandl & store in pandas
quandl.ApiConfig.api_key = 'P6LZzSkdVN6zTXQDE6Pd'
qGET = quandl.Dataset('NSE/OIL').data()
toPD = qGET.to_pandas()

# Generate a pandas DataFrame
pdDF = pd.DataFrame(toPD)

# Create a Spark DataFrame from a pandas DataFrame using Arrow base
sparkDF = spark.createDataFrame(pdDF)

# Rename the Column
dataDF = (sparkDF.withColumnRenamed("Total Trade Quantity", "TotalTradeQuantity")
                 .withColumnRenamed("Turnover (Lacs)","TurnoverLacs"))

# Create a Spark DataFrame Create Temp view
dataDF.createOrReplaceTempView("DimNSEOIL")

# Display database config
spark.sql("show databases").show()
spark.sql("use default").show()
spark.sql("show tables").show()

# Append & store Spark buffer
insertDF = spark.sql("select * from DimNSEOIL")
#insertDF.write.mode("append").saveAsTable("DimAppl")

#spark.sql("select * from DimAppl").show()

print('Reading in Progresql')
#--postgresql open database & Read Class Table
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "class") \
    .option("user", "postgres") \
    .option("password", "8784") \
    .option("driver", "org.postgresql.Driver") \
    .load()

print('Saving in Progresql')
# Saving data to a JDBC source
insertDF.write.mode("append") \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "public.DimTradeData") \
    .option("user", "postgres") \
    .option("password", "8784") \
    .option("driver", "org.postgresql.Driver") \
    .save()

print('Building Structure in Progresql')
#Create PySpark DataFrame Schema
p_schema = StructType([StructField('Open',StringType(),True),
                       StructField('High',StringType(),True),
                       StructField('Low',StringType(),True),
                       StructField('Last',StringType(),True),
                       StructField('Close',StringType(),True),
                       StructField('TotalTradeQuantity',StringType(),True),
                       StructField('TurnoverLacs',StringType(),True)])


spark.sql("select * from DimNSEOIL").show()