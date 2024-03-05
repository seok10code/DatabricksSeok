# Databricks notebook source
# MAGIC
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Delta Batch Operations Lab
# MAGIC
# MAGIC Databricks&reg; Delta allows you to read, write and query data in data lakes in an efficient manner.
# MAGIC
# MAGIC ## In this lesson you:
# MAGIC * Work with a traditional data pipeline using online shopping data
# MAGIC * Identify problems with the traditional data pipeline
# MAGIC * Use Databricks Delta features to mitigate those problems
# MAGIC
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers
# MAGIC * Secondary Audience: Data Analysts and Data Scientists
# MAGIC
# MAGIC ## Prerequisites
# MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and
# MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
# MAGIC * Databricks Runtime 4.2 or greater
# MAGIC * Completed courses Spark-SQL, DataFrames or ETL-Part 1 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>, or have similar knowledge
# MAGIC
# MAGIC ## Datasets Used
# MAGIC We will use online retail datasets from `/mnt/training/online_retail`

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###  READ CSV data then WRITE to Databricks Delta
# MAGIC
# MAGIC Read the data into a DataFrame. We suppply the schema.
# MAGIC
# MAGIC Use overwrite mode so that there will not be an issue in rewriting the data in case you end up running the cell again.
# MAGIC
# MAGIC Partition on `Country` because there are only a few unique countries and because we will use `Country` as a predicate in a `WHERE` clause.
# MAGIC
# MAGIC More information on the how and why of partitioning is contained in the links at the bottom of this notebook.
# MAGIC
# MAGIC Then write the data to Databricks Delta.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType

inputSchema = StructType([
  StructField("InvoiceNo", IntegerType(), True),
  StructField("StockCode", StringType(), True),
  StructField("Description", StringType(), True),
  StructField("Quantity", IntegerType(), True),
  StructField("InvoiceDate", StringType(), True),
  StructField("UnitPrice", DoubleType(), True),
  StructField("CustomerID", IntegerType(), True),
  StructField("Country", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1
# MAGIC
# MAGIC Read data in `outdoorSmallPath`. Re-use `inputSchema` as defined above.

# COMMAND ----------

# TODO

outdoorSmallPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-small.csv"
backfillDF = (spark
 .read
 .option("header", "true")
 FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution.

backfillCount = backfillDF.count()

dbTest("Delta-02-schemas", 99999, backfillCount)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Exercise 2
# MAGIC
# MAGIC Create a Databricks Delta table `backfill_data_delta` using the storage location `/delta/backfill-data/`.
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:**
# MAGIC * Don't forget to use overwrite mode just in case
# MAGIC * Partititon by `Country`

# COMMAND ----------

# TODO
(backfillDF
 .write
 .mode("overwrite")
 FILL_IN

spark.sql("""
   DROP TABLE IF EXISTS backfill_data_delta
 """)
spark.sql("""
   CREATE TABLE backfill_data_delta
FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution.
try:
  tableExists = (spark.table("backfill_data_delta") is not None)
except:
  tableExists = False

dbTest("Delta-02-backfillTableExists", True, tableExists)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3
# MAGIC
# MAGIC Count number of records from `backfill_data_delta` where the `Country` is `Sweden`.

# COMMAND ----------

# TODO
count = spark.sql("FILL IN").collect()[0][0]

# COMMAND ----------

# TEST - Run this cell to test your solution.
dbTest("Delta-L2-backfillDataDelta-count", 2925, count)
print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4
# MAGIC
# MAGIC 0. Read the JSON data under `streamingEventPath` into a DataFrame
# MAGIC 0. Add a `date` column using `from_unixtime(col("time").cast('String'),'MM/dd/yyyy').cast("date"))`
# MAGIC 0. Add a `deviceId` column consisting of random numbers from 0 to 99 using this expression `expr("cast(rand(5) * 100 as int)")`
# MAGIC 0. Use the `repartition` method to split the data into 200 partitions
# MAGIC
# MAGIC Refer to  <a href="http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#" target="_blank">Pyspark function documentation</a>.

# COMMAND ----------

# TODO
from pyspark.sql.functions import expr, col, from_unixtime, to_date
streamingEventPath = "/mnt/training/structured-streaming/events/"

rawDataDF = (spark
 .read
  FILL_IN
 .repartition(200)

# COMMAND ----------

# TEST - Run this cell to test your solution.
from pyspark.sql.types import StructField, StructType, StringType, LongType, DateType, IntegerType

expectedSchema = StructType([
   StructField("action",StringType(), True),
   StructField("time",LongType(), True),
   StructField("date",DateType(), True),
   StructField("deviceId",IntegerType(), True),
])

dbTest("Delta-03-schemas", set(expectedSchema), set(rawDataDF.schema))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5
# MAGIC
# MAGIC Write out the raw data in Databricks Delta format to `/delta/iot-pipeline/` and create a Databricks Delta table called `demo_iot_data_delta`.

# COMMAND ----------

# TODO
(rawDataDF
 .write
 .mode("overwrite")
  FILL_IN

spark.sql("""
   DROP TABLE IF EXISTS demo_iot_data_delta
 """)
spark.sql("""
   CREATE TABLE demo_iot_data_delta
   FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution.
try:
  tableExists = (spark.table("demo_iot_data_delta") is not None)
except:
  tableExists = False

dbTest("Delta-03-tableExists", True, tableExists)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6
# MAGIC
# MAGIC Create a new DataFrame with columns `action`, `time`, `date` and `deviceId`. The columns contain the following data:
# MAGIC
# MAGIC * `action` contains the value `Open`
# MAGIC * `time` contains the Unix time cast into a long integer `cast(1529091520 as bigint)`
# MAGIC * `date` contains `cast('2018-06-01' as date)`
# MAGIC * `deviceId` contains a random number from 0 to 499 given by `expr("cast(rand(5) * 500 as int)")`

# COMMAND ----------

# TODO
from pyspark.sql.functions import expr

newDataDF = (spark.range(10000)
  .repartition(200)
  .selectExpr("'Open' as action", FILL_IN)
  .FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution.
total = newDataDF.count()

dbTest("Delta-03-newDataDF-count", 10000, total)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercise 7
# MAGIC
# MAGIC Append new data to `demo_iot_data_delta`.
# MAGIC
# MAGIC * Use `append` mode
# MAGIC * Save to `deltaIotPath`

# COMMAND ----------

# TODO
(newDataDF
 .write
FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution.
numFiles = spark.sql("SELECT count(*) as total FROM demo_iot_data_delta").collect()[0][0]

dbTest("Delta-03-numFiles", 110000 , numFiles)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercise 8
# MAGIC
# MAGIC Create a DataFrame out of the table `demo_iot_data_delta`.

# COMMAND ----------

# TODO
newDataDF =  spark.sql("FILL_IN")

# COMMAND ----------

# TEST - Run this cell to test your solution.
from pyspark.sql.types import StructField, StructType, StringType, LongType, DateType, IntegerType

expectedSchema = StructType([
   StructField("action",StringType(), True),
   StructField("time",LongType(), True),
   StructField("date",DateType(), True),
   StructField("deviceId",IntegerType(), True),
])

dbTest("Delta-04-schemas", set(expectedSchema), set(newDataDF.schema))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Exercise 9
# MAGIC
# MAGIC Create another dataframe where you change`action` to `Close` for `date = '2018-06-01' ` and `deviceId = 485`.
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Use `distinct`.
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Consider using `selectExpr()`, as we did in [Lesson 3]($./03-Append).

# COMMAND ----------

# TODO

newDeviceId485DF =  (newDataDF
 .selectExpr(FILL_IN)
 .FILL_IN
)

# COMMAND ----------

# TEST - Run this cell to test your solution.
actionCount = newDeviceId485DF.select("Action").count()

dbTest("Delta-L4-actionCount", 1, actionCount)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Exercise 10
# MAGIC
# MAGIC Write to a new Databricks Delta table that contains just our data to be upserted.
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You can adapt the SQL syntax for the upsert from our demo example, above.

# COMMAND ----------

# TODO
spark.sql("FILL_IN")
newDeviceId485DF.write.saveAsTable("FILL_IN")

# COMMAND ----------

# TEST - Run this cell to test your solution.
try:
  tableExists = (spark.table("iot_data_delta_to_upsert") is not None)
  count = spark.table("iot_data_delta_to_upsert").count()
except:
  tableExists = False

dbTest("Delta-04-demoIotTableExists", True, tableExists)
dbTest("Delta-04-demoIotTableHasRow", 1, count)


print("Tests passed!")

# COMMAND ----------

# MAGIC %sql
# MAGIC --TODO
# MAGIC MERGE INTO demo_iot_data_delta
# MAGIC USING iot_data_delta_to_upsert
# MAGIC FILL_IN

# COMMAND ----------

spark.sql("SELECT * FROM demo_iot_data_delta").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 11
# MAGIC
# MAGIC Count the number of items in `demo_iot_data_delta` where the `deviceId` is `485` and `action` is `Close`.

# COMMAND ----------

# TODO
count = spark.sql("FILL IN").collect()[0][0]

# COMMAND ----------

# TEST - Run this cell to test your solution.
dbTest("Delta-L4-demoiot-count", 17, count)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 12: OPTIMIZE and ZORDER
# MAGIC
# MAGIC Let's apply some of these optimizations to `../delta/customer-data/`.
# MAGIC
# MAGIC Our data is partitioned by `Country`.
# MAGIC
# MAGIC We want to query the data for `StockCode` equal to `22301`.
# MAGIC
# MAGIC We expect this query to be slow because we have to examine ALL OF `../delta/customer-data/` to find the desired `StockCode` and not just in one or two partitions.
# MAGIC
# MAGIC First, let's time the above query: you will need to form a DataFrame to pass to `preZorderQuery`.

# COMMAND ----------

# TODO
%timeit preZorderQuery = spark.sql("FILL_IN").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Compact the files and re-order by `StockCode`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC OPTIMIZE FILL_IN
# MAGIC ZORDER by (FILL_IN)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's time the above query again: you will need to form a DataFrame to pass to `postZorderQuery`.

# COMMAND ----------

# TODO
%timeit postZorderQuery = spark.sql("FILL_IN").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 13: VACUUM
# MAGIC
# MAGIC Count number of files before `VACUUM` for `Country=Sweden`.

# COMMAND ----------

# TODO
preNumFiles = len(dbutils.fs.ls(FILL_IN))

# COMMAND ----------

# TEST - Run this cell to test your solution.
dbTest("Delta-08-numFilesSweden-pre", True, preNumFiles > 1)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC Now, watch the number of files shrink as you perform `VACUUM`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC VACUUM FILL_IN

# COMMAND ----------

# MAGIC %md
# MAGIC Count how many files there are for `Country=Sweden`.

# COMMAND ----------

# TODO
postNumFiles = len(dbutils.fs.ls(FILL_IN))

# COMMAND ----------

# TEST - Run this cell to test your solution.
dbTest("Delta-08-numFilesSweden-post", 1, postNumFiles)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC
# MAGIC * <a href="https://docs.databricks.com/delta/delta-batch.html#" target="_blank">Table Batch Read and Writes</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
