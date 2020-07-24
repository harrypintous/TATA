from pyspark import SparkContext, SparkConf

from os.path import expanduser, join, abspath
from pyspark.shell import sqlContext
from pyspark.sql import SparkSession

# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder.master("local") \
    .appName("Predicting Fire Dept Calls") \
    .config("spark.executor.memory","6gb")\
    .config("spark.sql.warehouse.dir", 'jdbc:hive2://localhost:10000') \
    .enableHiveSupport() \
    .getOrCreate()

#from pyspark.sql import SparkSession
#spark = SparkSession.builder.enableHiveSupport().getOrCreate()

spark.sql("show databases").show()
spark.sql("use adventurework")
spark.sql("show tables").show()

spark.sql("select * from factcurrencyrate").show()
spark.sql("select * from DimCurrency").show()
spark.sql("select * from FactFinance").show()
spark.sql("select * from factcurrencyrate fc left join DimCurrency dc on dc.CurrencyKey = fc.CurrencyKey").show()

spark.sql("select * from DimEmployee").show()
