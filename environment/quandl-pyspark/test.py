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
import sqlite3
from sqlite3 import Error

from pyspark.sql import SQLContext
print(sc)

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

#Create Pandas DataFrame
pd_person = pd.DataFrame({'PERSONID':'0','LASTNAME':'Doe','FIRSTNAME':'John','ADDRESS':'Museumplein','CITY':'Amsterdam'}, index=[0])
#pd_person = pd.DataFrame({'ADDRESS':'Museumplein','CITY':'Amsterdam','FIRSTNAME':'John','LASTNAME':'Doe','PERSONID':'0'}, index=[0])

#Create PySpark DataFrame Schema
p_schema = StructType([StructField('ADDRESS',StringType(),True),
                       StructField('CITY',StringType(),True),
                       StructField('FIRSTNAME',StringType(),True),
                       StructField('LASTNAME',StringType(),True),
                       StructField('PERSONID',StringType(),True)])

#Create Spark DataFrame from Pandas
df_person = sqlContext.createDataFrame(pd_person, p_schema)
#Important to order columns in the same order as the target database
df_persons  = df_person.select("PERSONID", "LASTNAME", "FIRSTNAME", "CITY", "ADDRESS")

spark = SparkSession.builder.appName('pandasToSparkDF').getOrCreate()

df_persons.createOrReplaceTempView("DimSalary")
spark.sql("select * from DimSalary").show()

'''
spark = SparkSession.builder.appName('pandasToSparkDF').getOrCreate()
quandl.ApiConfig.api_key = 'P6LZzSkdVN6zTXQDE6Pd'

qGET = quandl.Dataset('NSE/OIL').data()
df = qGET.to_pandas()
df_pd = pd.DataFrame(df)

df = spark.createDataFrame(df_pd)
df.printSchema()

#Create PySpark DataFrame Schema
p_schema = StructType([StructField('Open',FloatType(),True),
                       StructField('High',FloatType(),True),
                       StructField('Low',FloatType(),True),
                       StructField('Last',FloatType(),True),
                       StructField('Close',FloatType(),True),
                       StructField('Total Trade Quantity',FloatType(),True),
                       StructField('Turnover (Lacs)',FloatType(),True)])

 
#Create Spark DataFrame from Pandas
df_person = sqlContext.createDataFrame(df_pd, p_schema)
#Important to order columns in the same order as the target database
df_person  = df_person.select("Open", "High", "Low", "Last", "Close")
df_person.show()

'''