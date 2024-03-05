// Databricks notebook source
// MAGIC
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Databricks Delta Batch Operations Lab
// MAGIC
// MAGIC Databricks&reg; Delta allows you to read, write and query data in data lakes in an efficient manner.
// MAGIC
// MAGIC ## In this lesson you:
// MAGIC * Work with a traditional data pipeline using online shopping data
// MAGIC * Identify problems with the traditional data pipeline
// MAGIC * Use Databricks Delta features to mitigate those problems
// MAGIC
// MAGIC ## Audience
// MAGIC * Primary Audience: Data Engineers
// MAGIC * Secondary Audience: Data Analysts and Data Scientists
// MAGIC
// MAGIC ## Prerequisites
// MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and
// MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
// MAGIC * Databricks Runtime 4.2 or greater
// MAGIC * Completed courses Spark-SQL, DataFrames or ETL-Part 1 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>, or have similar knowledge
// MAGIC
// MAGIC ## Datasets Used
// MAGIC We will use online retail datasets from `/mnt/training/online_retail`

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ###  READ CSV data then WRITE to Databricks Delta
// MAGIC
// MAGIC Read the data into a DataFrame. We suppply the schema.
// MAGIC
// MAGIC Use overwrite mode so that there will not be an issue in rewriting the data in case you end up running the cell again.
// MAGIC
// MAGIC Partition on `Country` because there are only a few unique countries and because we will use `Country` as a predicate in a `WHERE` clause.
// MAGIC
// MAGIC More information on the how and why of partitioning is contained in the links at the bottom of this notebook.
// MAGIC
// MAGIC Then write the data to Databricks Delta.

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, DoubleType, IntegerType, StringType}

lazy val inputSchema = StructType(List(
  StructField("InvoiceNo", IntegerType, true),
  StructField("StockCode", StringType, true),
  StructField("Description", StringType, true),
  StructField("Quantity", IntegerType, true),
  StructField("InvoiceDate", StringType, true),
  StructField("UnitPrice", DoubleType, true),
  StructField("CustomerID", IntegerType, true),
  StructField("Country", StringType, true)
))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 1
// MAGIC
// MAGIC Read data in `outdoorSmallPath`. Re-use `inputSchema` as defined above.

// COMMAND ----------

// TODO

val outdoorSmallPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-small.csv"

val backfillDF = spark
  .read
  .option("header", "true")
 FILL_IN

// COMMAND ----------

// TEST - Run this cell to test your solution.

val backfillCount = backfillDF.count()

dbTest("Delta-02-schemas", 99999, backfillCount)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Exercise 2
// MAGIC
// MAGIC Create a Databricks Delta table `backfill_data_delta` using the storage location `/delta/backfill-data/`.
// MAGIC
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:**
// MAGIC * Don't forget to use overwrite mode just in case
// MAGIC * Partititon by `Country`

// COMMAND ----------

// TODO
backfillDF
 .write
 .mode(SaveMode.Overwrite)
  FILL_IN

spark.sql(s"""
 DROP TABLE IF EXISTS backfill_data_delta
""")

spark.sql(s"""
 CREATE TABLE backfill_data_delta
 FILL_IN

// COMMAND ----------

// TEST - Run this cell to test your solution.
lazy val tableExists = spark.catalog.tableExists("backfill_data_delta")

dbTest("Delta-02-backfillTableExists", true, tableExists)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 3
// MAGIC
// MAGIC Count number of records from `backfill_data_delta` where the `Country` is `Sweden`.

// COMMAND ----------

// TODO
val count = spark.sql("FILL IN").as[Long].collect()(0)

// COMMAND ----------

// TEST - Run this cell to test your solution.
dbTest("Delta-L2-backfillDataDelta-count", 2925L, count)
println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 4
// MAGIC 0. Read the JSON data under `streamingEventPath` into a DataFrame
// MAGIC 0. Add a `date` column using `to_date(from_unixtime(col("time").cast("Long"),"yyyy-MM-dd"))`
// MAGIC 0. Add a `deviceId` column consisting of random numbers from 0 to 99 using this expression `expr("cast(rand(5) * 100 as int)")`
// MAGIC 0. Use the `repartition` method to split the data into 200 partitions
// MAGIC
// MAGIC Refer to  <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$#" target="_blank">Spark Scala function documentation</a>.

// COMMAND ----------

// TODO
import org.apache.spark.sql.functions.{expr, from_unixtime, to_date}

val streamingEventPath = "/mnt/training/structured-streaming/events/"

rawDataDF = spark
 .read
 FILL_IN
 .repartition(200)

// COMMAND ----------

// TEST - Run this cell to test your solution.
import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType, DateType, IntegerType}

lazy val expectedSchema = StructType(
  List(
   StructField("action", StringType, true),
   StructField("time", LongType, true),
   StructField("date", DateType, true),
   StructField("deviceId", IntegerType, true)
))

dbTest("Delta-03-schemas", Set(expectedSchema), Set(rawDataDF.schema))

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 5
// MAGIC
// MAGIC Write out the raw data in Databricks Delta format to `/delta/iot-pipeline/` and create a Databricks Delta table called `demo_iot_data_delta`.

// COMMAND ----------

// TODO
rawDataDF
   .write
   .mode("overwrite")
   FILL_IN

spark.sql(s"""
   DROP TABLE IF EXISTS demo_iot_data_delta
 """)
spark.sql(s"""
   CREATE TABLE demo_iot_data_delta
   FILL_IN

// COMMAND ----------

// TEST - Run this cell to test your solution.

lazy val tableExists = spark.catalog.tableExists("demo_iot_data_delta")

dbTest("Delta-03-demoIotTableExists", true, tableExists)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 6
// MAGIC
// MAGIC Create a new DataFrame with columns `action`, `time`, `date` and `deviceId`. The columns contain the following data:
// MAGIC
// MAGIC * `action` contains the value `Open`
// MAGIC * `time` contains the Unix time cast into a long integer `cast(1529091520 as bigint)`
// MAGIC * `date` contains `cast('2018-06-01' as date)`
// MAGIC * `deviceId` contains a random number from 0 to 499 given by `expr("cast(rand(5) * 500 as int)")`

// COMMAND ----------

// TODO
import org.apache.spark.sql.functions.expr

val newDataDF = spark.range(10000)
 .repartition(200)
 .selectExpr("'Open' as action", FILL_IN)
 .FILL_IN

// COMMAND ----------

// TEST - Run this cell to test your solution.
lazy val total = newDataDF.count()

dbTest("Delta-03-newDataDF-count", 10000, total)
println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ## Exercise 7
// MAGIC
// MAGIC Append new data to `demo_iot_data_delta`.
// MAGIC
// MAGIC * Use `append` mode
// MAGIC * Save to `deltaIotPath`

// COMMAND ----------

// TODO
newDataDF
 .write
 FILL_IN

// COMMAND ----------

// TEST - Run this cell to test your solution.
lazy val numFiles = spark.sql("SELECT count(*) as total FROM demo_iot_data_delta").collect()(0)(0)

dbTest("Delta-03-numFiles", 110000 , numFiles)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ## Exercise 8
// MAGIC
// MAGIC Create a DataFrame out of the table `demo_iot_data_delta`.

// COMMAND ----------

// TODO
val newDataDF = spark.sql("FILL_IN")

// COMMAND ----------

// TEST - Run this cell to test your solution.
import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType, DateType, IntegerType}

lazy val expectedSchema = StructType(
  List(
   StructField("action", StringType, true),
   StructField("time", LongType, true),
   StructField("date", DateType, true),
   StructField("deviceId", IntegerType, true)
))

dbTest("Delta-04-schemas", Set(expectedSchema), Set(newDataDF.schema))

println("Tests passed!")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Exercise 9
// MAGIC
// MAGIC Create another dataframe where you change`action` to `Close` for `date = '2018-06-01' ` and `deviceId = 485`.
// MAGIC
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Use `distinct`.
// MAGIC
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Consider using `selectExpr()`, as we did in [Lesson 3]($./03-Append).

// COMMAND ----------

// TODO
newDeviceId485DF = newDataDF
 .selectExpr(FILL_IN)
 .FILL_IN

// COMMAND ----------

// TEST - Run this cell to test your solution.
lazy val actionCount = newDeviceId485DF.select("Action").count()

dbTest("Delta-L4-actionCount", 1, actionCount)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Exercise 10
// MAGIC
// MAGIC Write to a new Databricks Delta table that contains just our data to be upserted.
// MAGIC
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You can adapt the SQL syntax for the upsert from our demo example, above.

// COMMAND ----------

// TODO
spark.sql("FILL_IN")
newDeviceId485DF.write.saveAsTable("FILL_IN")

// COMMAND ----------

// TEST - Run this cell to test your solution.
lazy val tableExists = spark.catalog.tableExists("demo_iot_data_delta")
lazy val count = spark.table("iot_data_delta_to_upsert").count()

dbTest("Delta-04-demoIotTableExists", true, tableExists)
dbTest("Delta-04-demoIotTableHasRow", 1, count)

println("Tests passed!")

// COMMAND ----------

// MAGIC %sql
// MAGIC --TODO
// MAGIC MERGE INTO demo_iot_data_delta
// MAGIC USING iot_data_delta_to_upsert
// MAGIC FILL_IN

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 11
// MAGIC
// MAGIC Count the number of items in `demo_iot_data_delta` where the `deviceId` is `485` and `action` is `Close`.

// COMMAND ----------

// TODO
val count = spark.sql("FILL IN").collect()(0)(0)

// COMMAND ----------

// TEST - Run this cell to test your solution.
dbTest("Delta-L4-demoiot-count", 17, count)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 12: OPTIMIZE and ZORDER
// MAGIC
// MAGIC Let's apply some of these optimizations to `../delta/customer-data/`.
// MAGIC
// MAGIC Our data is partitioned by `Country`.
// MAGIC
// MAGIC We want to query the data for `StockCode` equal to `22301`.
// MAGIC
// MAGIC We expect this query to be slow because we have to examine ALL OF `../delta/customer-data/` to find the desired `StockCode` and not just in one or two partitions.
// MAGIC
// MAGIC First, let's time the above query: you will need to form a DataFrame to pass to `preZorderQuery`.

// COMMAND ----------

// TODO
def timeIt[T](op: => T): Float = {
 val start = System.currentTimeMillis
 val res = op
 val end = System.currentTimeMillis
 (end - start) / 1000.toFloat
}

val preZorderQuery = timeIt(spark.sql("FILL_IN").collect())

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Compact the files and re-order by `StockCode`.

// COMMAND ----------

// MAGIC %sql
// MAGIC -- TODO
// MAGIC OPTIMIZE FILL_IN
// MAGIC ZORDER by (FILL_IN)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's time the above query again: you will need to form a DataFrame to pass to `postZorderQuery`.

// COMMAND ----------

// TODO
val postZorderQuery = timeIt(spark.sql("FILL_IN").collect())

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 13: VACUUM
// MAGIC
// MAGIC Count number of files before `VACUUM` for `Country=Sweden`.

// COMMAND ----------

// TODO
val preNumFiles = dbutils.fs.ls(FILL_IN).length

// COMMAND ----------

// TEST - Run this cell to test your solution.
dbTest("Delta-08-numFilesSweden-pre", true, preNumFiles > 1)
println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC Now, watch the number of files shrink as you perform `VACUUM`.

// COMMAND ----------

// MAGIC %sql
// MAGIC -- TODO
// MAGIC VACUUM FILL_IN

// COMMAND ----------

// MAGIC %md
// MAGIC Count how many files there are for `Country=Sweden`.

// COMMAND ----------

// TODO
val postNumFiles = dbutils.fs.ls(FILL_IN).length

// COMMAND ----------

// TEST - Run this cell to test your solution.
dbTest("Delta-08-numFilesSweden-post", 1, postNumFiles)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Additional Topics & Resources
// MAGIC
// MAGIC * <a href="https://docs.databricks.com/delta/delta-batch.html#" target="_blank">Table Batch Read and Writes</a>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
