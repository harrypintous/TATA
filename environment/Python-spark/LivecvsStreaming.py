from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


dsfdsfsdfsdfsdfdsfdsfsdfsdf
spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()


#  https://www.quandl.com/api/v3/datatables/NDAQ/DL.csv?api_key=P6LZzSkdVN6zTXQDE6Pd


# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", " https://www.quandl.com/api/v3/datatables/NDAQ/DL.csv?api_key=P6LZzSkdVN6zTXQDE6Pd") \
    .option("port", 443) \
    .load()

# Split the lines into words
words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word")
)

#data = quandl.get_table("NDAQ/DL", paginate=True)

# Generate running word count
wordCounts = words.groupBy("word").count()


 # Start running the query that prints the running counts to the console
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

