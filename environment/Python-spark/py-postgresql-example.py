
from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
from pyspark.sql import Row

# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()
#spark.sql("drop table DimCurrency")

spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "class") \
    .option("user", "postgres") \
    .option("password", "8784") \
    .option("driver", "org.postgresql.Driver") \
    .load()


spark.sql("select * from class")


'''
# spark is an existing SparkSession
spark.sql(
    "CREATE TABLE IF NOT EXISTS DimCurrency(CurrencyKey INT, CurrencyAlternateKey char(3),CurrencyName VARCHAR(50)   ) USING hive")
spark.sql("LOAD DATA LOCAL INPATH '/home/hpinto/Desktop/mySpark-data/DimCurrency.csv' INTO TABLE DimCurrency")

# Queries are expressed in HiveQL
spark.sql("SELECT * FROM DimCurrency").show()
# +---+-------+
# |key|  value|
# +---+-------+
# |238|val_238|
# | 86| val_86|
# |311|val_311|
#

# Aggregation queries are also supported.
spark.sql("SELECT COUNT(*) FROM DimCurrency").show()
# +--------+
# |count(1)|
# +--------+
# |    500 |
# +--------+

# The results of SQL queries are themselves DataFrames and support all normal functions.
sqlDF = spark.sql("SELECT key, value FROM DimCurrency WHERE key < 10 ORDER BY key")

# The items in DataFrames are of type Row, which allows you to access each column by ordinal.
stringsDS = sqlDF.rdd.map(lambda row: "Key: %d, Value: %s" % (row.key, row.value))
for record in stringsDS.collect():
    print(record)
# Key: 0, Value: val_0
# Key: 0, Value: val_0
# Key: 0, Value: val_0
#

# You can also use DataFrames to create temporary views within a SparkSession.
Record = Row("key", "value")
recordsDF = spark.createDataFrame([Record(i, "val_" + str(i)) for i in range(1, 101)])
recordsDF.createOrReplaceTempView("records")

# Queries can then join DataFrame data with data stored in Hive.
spark.sql("SELECT * FROM records r JOIN DimCurrency s ON r.key = s.key").show()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "account") \
    .option("user", "postgres") \
    .option("password", "8784!") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# +---+------+---+------+
# |key| value|key| value|
# +---+------+---+------+
# |  2| val_2|  2| val_2|
# |  4| val_4|  4| val_4|
# |  5| val_5|  5| val_5|
#
'''