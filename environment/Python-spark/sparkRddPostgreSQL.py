#import warnings

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

print('Processing spark  part F')
# Display database config
spark.sql("show databases").show()

dfClass.createOrReplaceTempView("dclass")
dfPerson.createOrReplaceTempView("dperson")
#spark.sql("select * from dclass limit 10").show()
#spark.sql("select * from dperson limit 10").show()

dfPerson.join(dfClass, "name").show(10)

spark.sql("select * from dclass d1 inner join dperson d2 on d1.name = d2.name").show()