from pyspark import SparkContext, SparkConf
from os.path import expanduser, join, abspath
from pyspark.shell import sqlContext
from pyspark.sql import SparkSession

# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('../quandl-pyspark/spark-warehouse')

spark = SparkSession \
    .builder.master("local") \
    .appName("Predicting Fire Dept Calls") \
    .config("spark.executor.memory","16gb")\
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

spark2 = SparkSession \
    .builder.master("local") \
    .appName("Predicting Fire Dept Calls") \
    .config("spark.executor.memory","16gb")\
    .getOrCreate()

#df1 = spark2.read.load("/home/hpinto/Desktop/mySpark-data/DimCustomer.csv",
#                     format="csv", sep="|", inferSchema="true", header="false")

#df1.createOrReplaceTempView("dclass")
#spark.sql("select * from dclass").show()

df = spark2.read.format('com.databricks.spark.csv') \
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

df.createOrReplaceTempView("DimSalary")
spark.sql("select  * from DimSalary").show()
spark.sql("select f.EmployeeID, f.DateFirstPurchase, f.YearlyIncome from (select EmployeeID, max(YearlyIncome) as maxYearlyIncome from DimSalary group by EmployeeID) as x inner join DimSalary as f on f.EmployeeID = x.EmployeeID and f.YearlyIncome = x.maxYearlyIncome").show()
spark.sql("select f.EmployeeID, f.DateFirstPurchase, f.YearlyIncome from (select EmployeeID, max(DateFirstPurchase) as maxDateFirstPurchase from DimSalary group by EmployeeID) as x inner join DimSalary as f on f.EmployeeID = x.EmployeeID and f.DateFirstPurchase = x.maxDateFirstPurchase").show()

spark.sql("show databases").show()
#spark.sql("use adventurework")
#spark.sql("show tables").show()

#spark.sql("CREATE TABLE IF NOT EXISTS DimCurrency(CurrencyKey INT,CurrencyAlternateKey CHAR(3),CurrencyName VARCHAR(50))
# COMMENT 'DimCurrency details' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE")

#spark.sql("CREATE TABLE IF NOT EXISTS FactCurrencyRate (CurrencyKey INT,DateKey INT,AverageRate FLOAT,EndOfDayRate FLOAT,Date TIMESTAMP) COMMENT 'FactCurrencyRate details' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE")
#spark.sql("CREATE TABLE IF NOT EXISTS FactFinance(FinanceKey INT,DateKey int,OrganizationKey INT,DepartmentGroupKey INT,ScenarioKey INT,AccountKey INT,Amount FLOAT,Datex date ) COMMENT 'FactFinance details' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE")


#spark.sql("DROP TABLE IF EXISTS 123DimCustomer")
#spark.sql("CREATE TABLE IF NOT EXISTS 123DimCustomer(EmployeeKey INT, ParentEmployeeKey INT, EmployeeNationalIDAlternateKey VARCHAR(15), ParentEmployeeNationalIDAlternateKey VARCHAR(15), SalesTerritoryKey INT, FirstName VARCHAR(50), LastName VARCHAR(50), MiddleName VARCHAR(50), NameStyle BOOLEAN, Title VARCHAR(50), HireDate DATE,BirthDate DATE, LoginID VARCHAR(256), EmailAddress VARCHAR(50), Phone VARCHAR(25), MaritalStatus CHAR(1), EmergencyContactName VARCHAR(50), EmergencyContactPhone VARCHAR(25), SalariedFlag BOOLEAN, Gender CHAR(1), PayFrequency TINYINT,BaseRate DECIMAL(10,2), VacationHours SMALLINT, SickLeaveHours SMALLINT, CurrentFlag BOOLEAN,SalesPersonFlag BOOLEAN, DepartmentName VARCHAR(50), StartDate DATE, EndDate DATE, Status VARCHAR(50), EmployeePhoto STRING ) COMMENT '123DimEmployee details' ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' STORED AS TEXTFILE")



#spark.sql("LOAD DATA LOCAL INPATH '/home/hpinto/Desktop/mySpark-data/DimCurrency.csv' INTO TABLE DimCurrency")
#spark.sql("LOAD DATA LOCAL INPATH '/home/hpinto/Desktop/mySpark-data/FactCurrencyRate.csv' INTO TABLE factcurrencyrate")
#spark.sql("LOAD DATA LOCAL INPATH '/home/hpinto/Desktop/mySpark-data/FactFinance.csv' INTO TABLE FactFinance")
#spark.sql("LOAD DATA LOCAL INPATH '/home/hpinto/Desktop/mySpark-data/DimCustomer.csv' INTO TABLE DimCustomer")


#spark.sql("select * from DimCustomer").show()






