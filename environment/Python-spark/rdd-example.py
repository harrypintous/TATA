
# sc is an existing SparkContext. using registered tables
#SparkSession a combination of SQLContext, HiveContext and streamingContext
from pyspark.sql import SparkSession

spark = SparkSession\
    .builder \
    .master("local") \
    .appName("RDD-Sample") \
    .config("spark.executor.instances", "2")\
    .config("spark.executor.cores", "2")\
    .config("spark.executor.memory", "2g")\
    .config("spark.driver.memory", "2g") \
    .config("hive.merge.mapfiles", "false") \
    .config("hive.merge.tezfiles", "false")  \
    .config("parquet.enable.summary-metadata", "false") \
    .config("spark.sql.parquet.mergeSchema", "false") \
    .config("hive.merge.smallfiles.avgsize", "160000000")  \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.sql.orc.impl", "native") \
    .config("spark.sql.parquet.binaryAsString", "true") \
    .config("spark.sql.parquet.writeLegacyFormat", "true") \
    .enableHiveSupport()  \
    .getOrCreate()

#spark.getconf().getall()
#spark.sparkContext.getConf().getAll()


d = [{'name': 'Alice', 'age': 1}]
spark.createDataFrame(d).collect()