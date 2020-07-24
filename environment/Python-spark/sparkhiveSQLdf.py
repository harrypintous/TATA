from os.path import expanduser, join, abspath
from pyspark.shell import sqlContext
from pyspark.sql import SparkSession



# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Predicting Fire Dept Calls") \
    .config("spark.executor.memory","6gb")\
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .set("spark.network.timeout","12000s")\
    .getOrCreate()

#cx = spark.catalog.listTables("spark-warehouse")

df = spark.read.format('com.databricks.spark.csv') \
    .options(header='false', inferschema='true')\
    .load('/home/hpinto/Desktop/mySpark-data/DimCurrency.csv') \
    .toDF("CCkey", "CCcode","CCname")

df.createOrReplaceTempView("dim_curr")

# List all tables in Spark's catalog
#print(spark.catalog.listTables())

spark.sql("show tables").show()

#(df.write.format("orc")
#    .option("orc.bloom.filter.CCcode", "IND")
#    .option("orc.dictionary.key.threshold", "1.0")
#    .sortBy("CCname")
#    .sortByKey(ascending=False)
#    .save("trainingXxxx.orc"))
print("Training Table")
df1 = spark.read.load("trainingX.orc",
                     format="csv", sep=",", inferSchema="true", header="true")
df1.show()


spark.sql("show tables").show()

spark.sql("select CCkey, CCcode, CCname from dim_curr limit 10").show()

spark.sql("select CCkey, CCcode, CCname from dim_curr where CCkey = 1") \
    .show(10)

spark.sql("use default")
spark.sql("select * from employees").show(1000000)


df.write.format("orc").mode("append").saveAsTable("employees")
spark.sql("select * from employees").show(10)
spark.sql("show tables").show()




