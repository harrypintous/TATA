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
from pyspark.shell import sqlContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

# ----------------Import pandas and sqlite3
import pandas as pd
import sqlite3
from sqlite3 import Error

#from pyspark.sql.types import *

# Auxiliar functions
def equivalent_type(f):
    if f == 'datetime64[ns]': return DateType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return FloatType()
    else: return StringType()

def define_structure(string, format_type):
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)

# Given pandas dataframe, it will return a spark's dataframe.
def pandas_to_spark(pandas_df):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types):
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return sqlContext.createDataFrame(pandas_df, p_schema)




def create_connection_mem():  # """create a database connection to a database that resides in the memory"""
    conn = None
    try:
        conn = sqlite3.connect(':memory:')
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


def create_connection(db_file):  # """create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


class lookdata:

    def lookdataset(self, code):  # Convert xquandl to pandas format
        return quandl.Dataset(code).data()


class pandaset:
    def lookpandaset(self, code):
        return lookdata().lookdataset(code).to_pandas()





### Get data thru Function Get()
class lookgetdata:
    def lookget(self, code):  # Convert xquandl to pandas format
        return quandl.get(code).data()

class pandaget:
    def topandaget(self, code):
        return lookgetdata().lookget(code).to_pandas()


'''

from datetime import date

# random Person
class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    @classmethod
    def fromBirthYear(cls, name, birthYear):
        return cls(name, date.today().year - birthYear)

    def display(self):
        print(self.name + "'s age is: " + str(self.age))

person = Person('Adam', 19)
person.display()

person1 = Person.fromBirthYear('John',  1985)
person1.display()

 '''

# ------------------------------------------
from sqlalchemy import create_engine

engine = create_engine('sqlite://', echo=False)
# ------------------------------------------
# create an instance of SparkSession object
# ------------------------------------------
warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder.master("local") \
    .appName("Predicting Fire Dept Calls") \
    .config("spark.executor.memory", "16gb") \
    .config("spark.eventLog.enabled=true") \
    .config("spark.eventLog.dir=file:///home/hpinto/log") \
    .getOrCreate()

sc = spark.sparkContext

#    .config("spark.sql.warehouse.dir", warehouse_location) \
#    .enableHiveSupport() \

if __name__ == '__main__':
    create_connection(r"pythonsqlite.db")
    quandl.ApiConfig.api_key = 'P6LZzSkdVN6zTXQDE6Pd'

    qGET = quandl.Dataset('NSE/OIL').data()
    pandas_df = qGET.to_pandas()
    print(pandas_to_spark(pandas_df))


    columns = list(pandas_df.columns)
    print('get columns', columns)

    types = list(pandas_df.dtypes)
    print('get types', types)

    struct_list = []
    for column, typo in zip(columns, types):
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    print('get p_schema', p_schema)

    spark_DF = sqlContext.createDataFrame(pandas_df, p_schema)
    print('get spark_DF', spark_DF)








    '''
    # Connect to sqlite3 database
    conn = sqlite3.connect("pythonsqlite.db")
    cur = conn.cursor()

    # look dataset in quandl &  transfter to pandas
    df = pandaset().lookpandaset('WIKI/AAPL')

    #  pandas dataFrame store to Sqlite3
    df.to_sql("daily_flights", conn, if_exists="replace")
    pd_daily_flights = pd.read_sql_query("select * from daily_flights limit 10;", conn)
    print('daily_flights', pd_daily_flights)

    getdata = quandl.get('FRED/GDP',  start_date='2010-01-01', end_date='2014-01-01',
                      collapse='annual', transformation='rdiff',
                      rows=4)
    print('get data', getdata)

    datanse = quandl.get('NSE/OIL', start_date='2010-01-01', end_date='2014-01-01',
                      collapse='annual', transformation='rdiff',
                      rows=4)
    print('get datanse', datanse)





#-------------------------------zacksfc-start----------------------------------
    # Convert quandl to pandas format
    zacksfc = quandl.get_table('ZACKS/FC',
    qopts={'columns':['m_ticker','ticker','comp_name','comp_name_2','exchange','currency_code','per_end_date','per_type','per_code','per_fisc_year','per_fisc_qtr','per_cal_year','per_cal_qtr']})

#    print('gettable_zacksfc',zacksfc)
    #  pandas dataFrame store to Sqlite3
    df.to_sql("zacksfc", conn, if_exists="replace")
#    pd_zacksfc = pd.read_sql_query("select * from zacksfc limit 10;", conn)
#    print('zacksfc', pd_zacksfc)
  # SQL Execute records from tables
    sql = "SELECT * FROM `zacksfc`"
    cur.execute(sql)
    # Fetch all row by row records and use a for loop to print them one line at a time
    result = cur.fetchall()
    for i in result:
        print(i)


    sql = "SELECT * FROM `daily_flights`"
    cur.execute(sql)
    # Fetch all row by row records and use a for loop to print them one line at a time
    result = cur.fetchall()
    for i in result:
        print(i)



    # Temp store panda dataFrame & use sql statement
    pd_df = pd.DataFrame(df)
    pd_df.to_sql('zacksfc', con=engine)
    pd_cols = pd.DataFrame(df).columns

    # SQL Execute records from tables
    sql = "SELECT * FROM `zacksfc`"
    cur.execute(sql)
    # Fetch all row by row records and use a for loop to print them one line at a time
    result = cur.fetchall()
    for i in result:
        print(i)

# -------------------------------zacksfc-end----------------------------------


    # SQL Execute records from tables
    sql = "SELECT * FROM `daily_flights`"
    cur.execute(sql)
    # Fetch all row by row records and use a for loop to print them one line at a time
    result = cur.fetchall()
    for i in result:
        print(i)

# Convert quandl to pandas format
# data = quandl.Dataset('WIKI/AAPL').data()
# df = data.to_pandas()

 
# Temp store panda dataFrame & use sql statement 
pd_df = pd.DataFrame(df)
pd_df.to_sql('users', con =engine)
pd_cols = pd.DataFrame(df).columns
print(engine.execute("SELECT * FROM users").fetchall(),pd_cols)
pd_df.to_sql("select * from daily_flights",conn)







print('WIKI')
print(pd.DataFrame(df).columns)






# ------------------------------------------------
# Displays the content of the DataFrame to stdout
# ------------------------------------------------
df.createOrReplaceTempView("DimSalary")
spark.sql("select  * from DimSalary   ").show(1)




data = quandl.datatable.bulk_download_url("ZACKS/EE", ticker="AAPL")
print('Bulk-download',data)


#python
data = quandl.get('NSE/OIL')
print(data)




#QUANDL_API_KEY = quandl.ApiConfig.api_key = 'P6LZzSkdVN6zTXQDE6Pd'
symbol = 'WIKI/AAPL'  # or 'AAPL.US'
df = web.DataReader(symbol, 'quandl', '2015-01-01', '2020-01-05',api_key='P6LZzSkdVN6zTXQDE6Pd')

print('All DF',df)
data = web.DataReader( symbol, 'quandl',  start=2005, end=2008,api_key='P6LZzSkdVN6zTXQDE6Pd')
print(data)



ecno = web.DataReader('ticker=RGDPUS','econdb' )

print('Ecno',ecno.head())  '''