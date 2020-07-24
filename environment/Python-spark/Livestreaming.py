
from pyspark import SparkContext, SparkConf
from pyspark.shell import sc

data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
distData.collect()

#data = [1, 2, 3, 4, 5]
#distData = sc.parallelize(data)
#print(distData.reduce(lambda a, b: a + b))

#csv_data = sc.csv("file:///home/hpinto/Desktop/mycloud/DimCurrency.csv")
#csv_data  = csv_data.map(lambda p: p.split(","))
#df_csv = csv_data.map(lambda p: Row(CurrencyKey= p[0],CurrencyAlternateKey = p[1],CurrencyName = p[2])).toDF()

#df_csv.write.format("orc").saveAsTable("employeesZZZ")