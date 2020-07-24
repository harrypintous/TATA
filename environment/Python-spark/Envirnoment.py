 val conf  = new SparkConf().setAppName("Spark-JDBC")\
     .set("spark.executor.heartbeatInterval","120s")\
     .set("spark.network.timeout","12000s")\
     .set("spark.sql.inMemoryColumnarStorage.compressed", "true")\
     .set("spark.sql.orc.filterPushdown","true").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
     .set("spark.kryoserializer.buffer.max","512m").set("spark.serializer", classOf[org.apache.spark.serializer.KryoSerializer].getName)\
     .set("spark.streaming.stopGracefullyOnShutdown","true")\
     .set("spark.yarn.driver.memoryOverhead","7168")\
     .set("spark.yarn.executor.memoryOverhead","7168")\
     .set("spark.sql.shuffle.partitions", "61")\
     .set("spark.default.parallelism", "60")\
     .set("spark.memory.storageFraction","0.5")\
     .set("spark.memory.fraction","0.6")\
     .set("spark.memory.offHeap.enabled","true")\
     .set("spark.memory.offHeap.size","16g")\
     .set("spark.dynamicAllocation.enabled", "false")\
     .set("spark.dynamicAllocation.enabled","true")\
     .set("spark.shuffle.service.enabled","true")


  val spark = SparkSession.builder().config(conf).master("yarn")\
     .enableHiveSupport().config("hive.exec.dynamic.partition", "true")\
     .config("hive.exec.dynamic.partition.mode", "nonstrict")\
     .getOrCreate()


  def prepareFinalDF(splitColumns:List[String], textList: ListBuffer[String], allColumns:String, dataMapper:Map[String, String], partition_columns:Array[String], spark:SparkSession): DataFrame = {
        val colList                = allColumns.split(",").toList
        val (partCols, npartCols)  = colList.partition(p => partition_columns.contains(p.takeWhile(x => x != ' ')))
        val queryCols              = npartCols.mkString(",") + ", 0 as " + flagCol + "," + partCols.reverse.mkString(",")
        val execQuery              = s"select ${allColumns}, 0 as ${flagCol} from schema.tablename where period_year='2017' and period_num='12'"
        val yearDF                 = spark.read.format("jdbc").option("url", connectionUrl).option("dbtable", s"(${execQuery}) as year2017")
                                                                      .option("user", devUserName).option("password", devPassword)
                                                                      .option("partitionColumn","cast_id")
                                                                      .option("lowerBound", 1).option("upperBound", 100000)
                                                                      .option("numPartitions",70).load()
        val totalCols:List[String] = splitColumns ++ textList
        val cdt                    = new ChangeDataTypes(totalCols, dataMapper)
        hiveDataTypes              = cdt.gpDetails()
        val fc                     = prepareHiveTableSchema(hiveDataTypes, partition_columns)
        val allColsOrdered         = yearDF.columns.diff(partition_columns) ++ partition_columns
        val allCols                = allColsOrdered.map(colname => org.apache.spark.sql.functions.col(colname))
        val resultDF               = yearDF.select(allCols:_*)
        val stringColumns          = resultDF.schema.fields.filter(x => x.dataType == StringType).map(s => s.name)
        val finalDF                = stringColumns.foldLeft(resultDF) {
          (tempDF, colName) => tempDF.withColumn(colName, regexp_replace(regexp_replace(col(colName), "[\r\n]+", " "), "[\t]+"," "))
        }
        finalDF
  }
    val dataDF = prepareFinalDF(splitColumns, textList, allColumns, dataMapper, partition_columns, spark)
    val dataDFPart = dataDF.repartition(30)
    dataDFPart.createOrReplaceTempView("preparedDF")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql(s"INSERT OVERWRITE TABLE schema.hivetable PARTITION(${prtn_String_columns}) select * from preparedDF")
The data is inserted into the hive table dynamically partitioned based on prtn_String_columns: source_system_name, period_year, period_num

Spark-submit used:

SPARK_MAJOR_VERSION=2 spark-submit
 --conf spark.ui.port=4090
 --driver-class-path /home/fdlhdpetl/jars/postgresql-42.1.4.jar
 --jars /home/fdlhdpetl/jars/postgresql-42.1.4.jar
 --num-executors 80 --executor-cores 5 --executor-memory 50G
 --driver-memory 20G --driver-cores 3
 --class com.partition.source.YearPartition splinter_2.11-0.1.jar
 --master=yarn --deploy-mode=cluster --keytab /home/fdlhdpetl/fdlhdpetl.keytab
 --principal fdlhdpetl@FDLDEV.COM
 --files /usr/hdp/current/spark2-client/conf/hive-site.xml,testconnection.properties --name Splinter --conf spark.executor.extraClassPath=/home/fdlhdpetl/jars/postgresql-42.1.4.jar
The following error messages are generated in the executor logs:

Container exited with a non-zero exit code 143.
Killed by external signal
18/10/03 15:37:24 ERROR SparkUncaughtExceptionHandler: Uncaught exception in thread Thread[SIGTERM handler,9,system]
java.lang.OutOfMemoryError: Java heap space
    at java.util.zip.InflaterInputStream.<init>(InflaterInputStream.java:88)
    at java.util.zip.ZipFile$ZipFileInflaterInputStream.<init>(ZipFile.java:393)
    at java.util.zip.ZipFile.getInputStream(ZipFile.java:374)
    at java.util.jar.JarFile.getManifestFromReference(JarFile.java:199)
    at java.util.jar.JarFile.getManifest(JarFile.java:180)
    at sun.misc.URLClassPath$JarLoader$2.getManifest(URLClassPath.java:944)
    at java.net.URLClassLoader.defineClass(URLClassLoader.java:450)
    at java.net.URLClassLoader.access$100(URLClassLoader.java:73)
    at java.net.URLClassLoader$1.run(URLClassLoader.java:368)
    at java.net.URLClassLoader$1.run(URLClassLoader.java:362)
    at java.security.AccessController.doPrivileged(Native Method)
    at java.net.URLClassLoader.findClass(URLClassLoader.java:361)
    at java.lang.ClassLoader.loadClass(ClassLoader.java:424)
    at sun.misc.Launcher$AppClassLoader.loadClass(Launcher.java:331)
    at java.lang.ClassLoader.loadClass(ClassLoader.java:357)
    at org.apache.spark.util.SignalUtils$ActionHandler.handle(SignalUtils.scala:99)
    at sun.misc.Signal$1.run(Signal.java:212)
    at java.lang.Thread.run(Thread.java:745)
I see in the logs that the read is being executed properly with the given number of partitions as below:

Scan JDBCRelation((select column_names from schema.tablename where period_year='2017' and period_num='12') as year2017) [numPartitions=50]
Below is the state of executors in stages: enter image description here

enter image description here

enter image description here

enter image description here

The data is not being partitioned properly. One partition is smaller while the other one becomes huge. There is a skew problem here. While inserting the data into Hive table the job fails at the line:spark.sql(s"INSERT OVERWRITE TABLE schema.hivetable PARTITION(${prtn_String_columns}) select * from preparedDF") but I understand this is happening because of the data skew problem.

I tried to increase number of executors, increasing the executor memory, driver memory, tried to just save as csv file instead of saving the dataframe into a Hive table but nothing affects the execution from giving the exception:

java.lang.OutOfMemoryError: GC overhead limit exceeded
Is there anything in the code that I need to correct ? Could anyone let me know how can I fix this problem ?

apache-spark jdbc hive apache-spark-sql partitioning
shareimprove this question
edited Nov 22 '18 at 11:46

zero323
236k5757 gold badges689689 silver badges772772 bronze badges
asked Oct 2 '18 at 6:38

Metadata
1,44211 gold badge1919 silver badges4545 bronze badges
How many executors actually ran? – cricket_007 Oct 2 '18 at 8:17
Out of 50, 48 ran. – Metadata Oct 2 '18 at 8:29
@cricket_007 You have any suggestion for the problem ? – Metadata Oct 3 '18 at 10:29
Other than more executor memory or more executors, not really – cricket_007 Oct 3 '18 at 13:24
@cricket_007 I had these parameters as: --num-executors 50 --executor-cores 8 --executor-memory 60g but was still getting the same exception. Is there any action in the code that I need to avoid ? – Metadata Oct 3 '18 at 14:03
show 3 more comments
3 Answers
activeoldestvotes

8

+50
Determine how many partitions you need given the amount of input data and your cluster resources. As a rule of thumb it is better to keep partition input under 1GB unless strictly necessary. and strictly smaller than the block size limit.

You've previously stated that you migrate 1TB of data values you use in different posts (5 - 70) are likely way to low to ensure smooth process.

Try to use value which won't require further repartitioning.

Know your data.

Analyze the columns available in the the dataset to determine if there any columns with high cardinality and uniform distribution to be distributed among desired number of partitions. These are good candidates for an import process. Additionally you should determine an exact range of values.

Aggregations with different centrality and skewness measure as well as histograms and basic counts-by-key are good exploration tools. For this part it is better to analyze data directly in the database, instead of fetching it to Spark.

Depending on the RDBMS you might be able to use width_bucket (PostgreSQL, Oracle) or equivalent function to get a decent idea how data will be distributed in Spark after loading with partitionColumn, lowerBound, upperBound, numPartitons.

s"""(SELECT width_bucket($partitionColum, $lowerBound, $upperBound, $numPartitons) AS bucket, COUNT(*)
FROM t
GROUP BY bucket) as tmp)"""
If there are no columns which satisfy above criteria consider:

Creating a custom one and exposing it via. a view. Hashes over multiple independent columns are usually good candidates. Please consult your database manual to determine functions that can be used here (DBMS_CRYPTO in Oracle, pgcrypto in PostgreSQL)*.
Using a set of independent columns which taken together provide high enough cardinality.

Optionally, if you're going to write to a partitioned Hive table, you should consider including Hive partitioning columns. It might limit the number of files generated later.

Prepare partitioning arguments

If column selected or created in the previous steps is numeric (or date / timestamp in Spark >= 2.4) provide it directly as the partitionColumn and use range values determined before to fill lowerBound and upperBound.

If bound values don't reflect the properties of data (min(col) for lowerBound, max(col) for upperBound) it can result in a significant data skew so thread carefully. In the worst case scenario, when bounds don't cover the range of data, all records will be fetched by a single machine, making it no better than no partitioning at all.

If column selected in the previous steps is categorical or is a set of columns generate a list of mutually exclusive predicates that fully cover the data, in a form that can be used in a SQL where clause.

For example if you have a column A with values {a1, a2, a3} and column B with values {b1, b2, b3}:

val predicates = for {
  a <- Seq("a1", "a2", "a3")
  b <- Seq("b1", "b2", "b3")
} yield s"A = $a AND B = $b"
Double check that conditions don't overlap and all combinations are covered. If these conditions are not satisfied you end up with duplicates or missing records respectively.

Pass data as predicates argument to jdbc call. Note that the number of partitions will be equal exactly to the number of predicates.

Put database in a read-only mode (any ongoing writes can cause data inconsistency. If possible you should lock database before you start the whole process, but if might be not possible, in your organization).

If the number of partitions matches the desired output load data without repartition and dump directly to the sink, if not you can try to repartition following the same rules as in the step 1.

If you still experience any problems make sure that you've properly configured Spark memory and GC options.

If none of the above works:

Consider dumping your data to a network / distributes storage using tools like COPY TO and read it directly from there.

Note that or standard database utilities you will typically need a POSIX compliant file system, so HDFS usually won't do.

The advantage of this approach is that you don't need to worry about the column properties, and there is no need for putting data in a read-only mode, to ensure consistency.

Using dedicated bulk transfer tools, like Apache Sqoop, and reshaping data afterwards.

* Don't use pseudocolumns - Pseudocolumn in Spark JDBC.

shareimprove this answer
edited May 20 '19 at 5:27

Community♦
111 silver badge
answered Oct 6 '18 at 10:20

10465355 says Reinstate Monica
3,07922 gold badges77 silver badges2626 bronze badges
1
The code is running now. But I see a problem here. Some of the tables have composite primary keys i.e. two columns are mentioned as 'primary key'. For example: ref_id(integer), header_id(integer), source_system_name(String) are mentioned as primary keys. How do I prepare the partitionColumn in this scenario ? – Metadata Oct 9 '18 at 11:37
1
There is really no requirement for partitionColumn to be a primary key, so as long as individual components columns have properties described in point you can safely use any of these. If for some reason you want both you can for example compute something like ref_id << 32 | header_id and use the result as partitioning column. Or use some other bitwise operation like ref_id ⊕ header_id. To pass it to Spark you'll need either database view, or subquery – 10465355 says Reinstate Monica Oct 9 '18 at 15:39
Ok. One last thing. The code ran for table with just 1GB of data with a primary key column (integer datatype) and lower bound & upper bounds being the min & max values of that column. But when I try the same thing on a table of size 400gb, the job fails with GC overhead exception again. I'll post the spark-jdbc read in the below comment. – Metadata Oct 13 '18 at 12:19
val dataDF = spark.read.format("jdbc").option("url", connectionUrl).option("dbtable", s"(select * from schema.table where period_year=2016) as year2016") .option("user", usrName).option("password", pwd) .option("partitionColumn","header_id") .option("lowerBound", 3275L).option("upperBound", 1152921494159532424L) .option("numPartitions",100).load() I increased the num of partition, executor & driver memories but still face the same exception. If this also doesn't work, is there anything I can try to apply ? – Metadata Oct 13 '18 at 12:20
Is header_id uniformly distributed (after applying the period_year=2016 predicate)? How much did you increase the partitions? – 10465355 says Reinstate Monica Oct 13 '18 at 15:51
show 7 more comments


1

In my experience there are 4 kinds of memory settings which make a difference:

A) [1] Memory for storing data for processing reasons VS [2] Heap Space for holding the program stack

B) [1] Driver VS [2] executor memory

Up to now, I was always able to get my Spark jobs running successfully by increasing the appropriate kind of memory:

A2-B1 would therefor be the memory available on the driver to hold the program stack. Etc.

The property names are as follows:

A1-B1) executor-memory

A1-B2) driver-memory

A2-B1) spark.yarn.executor.memoryOverhead

A2-B2) spark.yarn.driver.memoryOverhead

Keep in mind that the sum of all *-B1 must be less than the available memory on your workers and the sum of all *-B2 must be less than the memory on your driver node.

My bet would be, that the culprit is one of the boldly marked heap settings.

shareimprove this answer
answered Oct 8 '18 at 13:31

Elmar Macek
33433 silver badges1111 bronze badges
add a comment

0

There was an another question of yours routed here as duplicate

 'How to avoid data skewing while reading huge datasets or tables into spark?
  The data is not being partitioned properly. One partition is smaller while the
  other one becomes huge on read.
  I observed that one of the partition has nearly 2million rows and
  while inserting there is a skew in partition. '
if the problem is to deal with data that is partitioned in a dataframe after read, Have you played around increasing the "numPartitions" value ?

.option("numPartitions",50)
lowerBound, upperBound form partition strides for generated WHERE clause expressions and numpartitions determines the number of split.

say for example, sometable has column - ID (we choose that as partitionColumn) ; value range we see in table for column-ID is from 1 to 1000 and we want to get all the records by running select * from sometable, so we going with lowerbound = 1 & upperbound = 1000 and numpartition = 4

this will produce a dataframe of 4 partition with result of each Query by building sql based on our feed (lowerbound = 1 & upperbound = 1000 and numpartition  = 4)

select * from sometable where ID < 250
select * from sometable where ID >= 250 and ID < 500
select * from sometable where ID >= 500 and ID < 750
select * from sometable where ID >= 750
what if most of the records in our table fall within the range of ID(500,750). that's the situation you are in to.

when we increase numpartition , the split happens even further and that reduce the volume of records in the same partition but this is not a fine shot.

Instead of spark splitting the partitioncolumn based on boundaries we provide, if you think of feeding the split by yourself so, data can be evenly splitted. you need to switch over to another JDBC method where instead of (lowerbound,upperbound & numpartition) we can provide predicates directly.

def jdbc(url: String, table: String, predicates: Array[String], connectionProperties: Properties): DataFrame
Link

shareimprove this answer
edited Oct 10 '18 at 18:16
answered Oct 7 '18 at 8:11

Karthick
52044 silver badges1414 bronze badges
The link you gave is not working. Can you give me the proper one ? – Metadata Oct 10 '18 at 17:36
Understood. Looking at the method you mentioned, I don't have to mention the bounds, but what exactly do I give here: "predicates: Condition in the where clause for each partition." Is it the same partitionColumn, lowBound, upperBound in array ? – Metadata Oct 10 '18 at 18:23
something like this, val query = Array[String]("column >= value1 and column < value2", "column >= value2 and column < value3","column >= value3 and column < value4", .......) . each one defines one partition of the DataFrame. – Karthick Oct 10 '18 at 18:25
Ok. But I have 22Bill rows in the table with various values in that partition column and I can't give the predicates for so many values. – Metadata Oct 10 '18 at 18:30
add a comment
Your Answer
Sign up or log in
 Sign up using Google
 Sign up using Facebook
 Sign up using Email and Password
Post as a guest
Name
Email
Required, but never shown

Post Your Answer
By clicking “Post Your Answer”, you agree to our terms of service, privacy policy and cookie policy

Not the answer you're looking for? Browse other questions tagged apache-spark jdbc hive apache-spark-sql partitioning or ask your own question.
Blog
How to create micro-interactions with react-spring: Part 1
This week, #StackOverflowKnows syntactic sugar, overfit or nah, and the…
Featured on Meta
Thank you, Robert Cartaino
Change in roles for Jon Ericson (leaving SE)
Has Stack Exchange rescinded moderator access to the featured tag on Meta?
How do the moderator resignations affect me and the community?

