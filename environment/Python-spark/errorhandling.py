import os
import sys

os.environ['SPARK_HOME'] = "/opt/spark"
sys.path.append("/home/spark/python")
sys.path.append("/home/spark/python/lib")
sys.path.append("/opt/spark/python/lib/py4j-0.10.8.1-src")

from pyspark.sql import SparkSession

import logging

def do_my_logging(log_msg):

    logger = logging.getLogger('__FILE__')
    logger.warning('log_msg = {}'.format(log_msg))

# initiazation of Sparksession
spark = SparkSession \
    .builder.master("local") \
    .appName("Predicting Fire Dept Calls") \
    .config("spark.eventLog.enabled=true")\
    .config("spark.eventLog.dir=file:///home/hpinto/log") \
    .getOrCreate()


sc = spark.sparkContext

log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.info("pyspark script logger initialized")

spark.sparkContext.setLogLevel('WARN')
spark.sparkContext.setLogLevel('INFO')
spark.sparkContext.setLogLevel('ERROR')


do_my_logging("spark.read.format(com.databricks.spark.csv")

df = spark.read.format('com.databricks.spark.csv') \
    .options(header='false', inferschema='true')\
    .load('/home/hpinto/Desktop/mySpark-data/DimCurrency.csv') \
    .toDF("CCkey", "CCcode","CCname")

print(df.isStreaming)

if format is not None:
    df.show()


print('Following')

'''
#    .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
try:
        df.first()
except Py4JJavaError as e:
        print(sc.emptyRDD())
        exit(0)

print('DimCurrency process')
if not df.count() < 1:
    df.show(5)

 
from py4j.protocol import Py4JJavaError

def try_load(path):
    rdd = sc.textFile(path)
    try:
        rdd.first()
        return rdd
    except Py4JJavaError as e:
        return sc.emptyRDD()

rdd = try_load(s3_path)
if not rdd.isEmpty():
    run_the_rest_of_your_code(rdd)



#do_my_logging("DISPLAY 1ST 20 RECORD")
#df.show(1)

#do_my_logging("select(CCkey, CCcode,CCname")
#df.select("CCkey", "CCcode","CCname").show(1)

#df.select('CCkey').distinct().show(1)
#df.groupBy('CCcode').count().show(1)
#df2 = df.groupBy('CCname').count()

#print("CCname")
#df.groupBy('CCname').count().orderBy('count', ascending=True).show(1)


#df.printSchema()



#from pyspark.sql import functions as F
#fireIndicator = df.select(df["CCkey"],F.when(df["CCcode"].like("%ALL%"),1)\
#                          .otherwise(0).alias('CCname'))
#fireIndicator.show(1)

#print('Rows without Null values')
#df.dropna().count()

#print('Row with Null Values')
#df.count()-df.dropna().count()

df = spark.read.format('com.databricks.spark.csv') \
    .options(header='false', format="csv", inferschema='true')\
   .option("mode", "DROPMALFORMED")\
    .load('/home/hpinto/Desktop/mySpark-data/DimCustomer.csv') \
   .toDF(
	   "EmployeeID"
	   ,"GeographyKey"
	   ,"CustomerAlternateKey"
	   ,"Title"
	   ,"FirstName"
	   ,"MiddleName"
	   ,"LastName"
	   ,"NameStyle"
	   ,"BirthDate"
	   ,"MaritalStatus"
	   ,"Suffix"
	   ,"Gender"
	   ,"EmailAddress"
	   ,"YearlyIncome"
	   ,"TotalChildren"
	   ,"NumberChildrenAtHome"
	   ,"EnglishEducation"
	   ,"SpanishEducation"
	   ,"FrenchEducation"
	   ,"EnglishOccupation"
	   ,"SpanishOccupation"
	   ,"FrenchOccupation"
	   ,"HouseOwnerFlag"
	   ,"NumberCarsOwned"
	   ,"AddressLine1"
	   ,"AddressLine2"
	   ,"Phone"
	   ,"DateFirstPurchase"
	   ,"CommuteDistance")

try:
    df.first()
except Py4JJavaError as e:
    print(sc.emptyRDD())
    exit(0)

print('DimCustomer process')
if not df.count() < 1:

    df.createOrReplaceTempView("DimSalary")
    spark.sql("select  * from DimSalary   ").show(1)
    spark.sql("select f.EmployeeID, f.DateFirstPurchase, f.YearlyIncome from ("
          "select EmployeeID, max(YearlyIncome) as maxYearlyIncome from DimSalary group by EmployeeID) as x "
          "inner join DimSalary as f on f.EmployeeID = x.EmployeeID and f.YearlyIncome = x.maxYearlyIncome").show()

    spark.sql("SELECT f.* FROM  (SELECT EmployeeID,YearlyIncome FROM DimSalary WHERE YearlyIncome > 90000) as x "
          "Left join DimSalary as f on f.EmployeeID = x.EmployeeID and f.YearlyIncome = x.YearlyIncome").show()

#SELECT name, salary FROM #Employee e1 WHERE N-1 = (SELECT COUNT(DISTINCT salary) FROM #Employee e2 WHERE e2.salary > e1.salary)
#Top N record
    spark.sql("select  YearlyIncome from DimSalary order by 1 desc ").show()
    spark.sql("with sal as (select Gender,YearlyIncome, dense_rank() OVER (PARTITION BY Gender ORDER BY YearlyIncome DESC) as rank "
		  "from DimSalary ) select Gender,YearlyIncome , rank from sal").show(100000)
    '''