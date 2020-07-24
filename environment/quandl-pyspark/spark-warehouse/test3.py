
# import sys
from pyspark.shell import sqlContext, sc
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *


from pyspark.sql import SQLContext
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation
from pyspark.ml import image

spark = SparkSession \
    .builder \
    .appName("Read Python Spark image") \
    .getOrCreate()


spark.sparkContext.setLogLevel('WARN')
spark.sparkContext.setLogLevel('INFO')
spark.sparkContext.setLogLevel('ERROR')


# path to your image source directory
sample_img_dir = "https://github.com/WeichenXu123/spark/tree/218ce4cf796308c8705a27889b25100e2b779365/data/mllib/images/origin/kittens"
# Read image data using new image scheme
image_df = spark.read.format("image").load(sample_img_dir)






''' 

print('Processing A part')
sample_img_dir = "https://github.com/WeichenXu123/spark/tree/218ce4cf796308c8705a27889b25100e2b779365/data/mllib/images/origin/kittens"
# Create image DataFrame using image data source in Apache Spark 2.4
image_df = spark.read.format("image").load(sample_img_dir)

# In Databricks Runtime 5.0 and above, the display command includes built-in image support
print(image_df)

print('Processing A1 part')

imageDF = spark.read.format("image") \
    .option("dropInvalid", True) \
    .load("https://github.com/WeichenXu123/spark/tree/218ce4cf796308c8705a27889b25100e2b779365/data/mllib/images/origin/kittens")
imageDF.printSchema()

print('Processing B part')
print('df',imageDF)

imageDF.select("image.origin", "image.width", "image.height").show(truncate=False)
print('df',imageDF) 
'''