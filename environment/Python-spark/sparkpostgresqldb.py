import os
import sysos.environ['SPARK_HOME'] = "/opt/spark"
sys.path.append("/home/spark/python")
sys.path.append("/home/spark/python/lib")
sys.path.append("/opt/spark/python/lib/py4j-0.10.8.1-src")

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.2.9.jar") \
    .config("spark.master", "local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')
spark.sparkContext.setLogLevel('INFO')
spark.sparkContext.setLogLevel('ERROR')

dfClass = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "class") \
    .option("user", "postgres") \
    .option("password", "8784") \
    .option("driver", "org.postgresql.Driver") \
    .load()

dfPerson = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "person") \
    .option("user", "postgres") \
    .option("password", "8784") \
    .option("driver", "org.postgresql.Driver") \
    .load()

dfClass.createOrReplaceTempView("dclass")
dfPerson.createOrReplaceTempView("dperson")
#spark.sql("select * from dclass limit 10").show()
#spark.sql("select * from dperson limit 10").show()


print('Processing spark  part F')
# Display database config
spark.sql("show databases").show()



dfPerson.join(dfClass, "name").show(10)

dfx=spark.sql("select * from dclass d1 inner join dperson d2 on d1.name = d2.name").show()

print("defalut data to a JDBC source")
dfPerson.write.format("orc").mode("append").saveAsTable("harryx")
spark.sql("select * from harryx").show(10)
spark.sql("show tables").show()

print("Saving data to a JDBC source")
dfPerson.write.mode("append") \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/tata") \
    .option("dbtable", "public.TradeDatabk2") \
    .option("user", "postgres") \
    .option("password", "8784") \
    .option("driver", "org.postgresql.Driver") \
    .save()