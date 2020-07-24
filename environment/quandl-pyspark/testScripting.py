import pyspark
import quandl
from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.appName('pandasToSparkDF').getOrCreate()

quandl.ApiConfig.api_key = 'P6LZzSkdVN6zTXQDE6Pd'

qGET = quandl.Dataset('NSE/OIL').data()
pdDF = qGET.to_pandas()

print(pdDF)

from pyspark.sql.types import *

columns = list(pdDF.columns)
print('get columns', columns)

types = list(pdDF.dtypes)
print('get types', types)

pdSchema = StructType([StructField(columns, types, True)])

'''
pdSchema = StructType([ StructField("Open", LongType(), True)\
                       ,StructField("High", IntegerType(), True)\
                       ,StructField("Low", IntegerType(), True)\
                       ,StructField("Close", IntegerType(), True)\
                       ,StructField("Total Trade Quantity", IntegerType(), True)\
                       ,StructField("Turnover (Lacs)", IntegerType(), True) ]) 
                       '''
