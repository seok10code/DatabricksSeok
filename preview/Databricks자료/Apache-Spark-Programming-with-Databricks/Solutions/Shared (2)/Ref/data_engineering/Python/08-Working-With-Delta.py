# Databricks notebook source
# MAGIC
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Delta Batch Operations
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

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC
# MAGIC You will notice that throughout this course, there is a lot of context switching between PySpark/Scala and SQL.
# MAGIC
# MAGIC This is because:
# MAGIC * `read` and `write` operations are performed on DataFrames using PySpark or Scala
# MAGIC * table creates and queries are performed directly off Databricks Delta tables using SQL
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Delta Batch Operations - Create
# MAGIC
# MAGIC ### Datasets Used
# MAGIC We will use online retail datasets from `/mnt/training/online_retail`

# COMMAND ----------

# MAGIC %md
# MAGIC Set up relevant paths.

# COMMAND ----------

inputPath = "/mnt/training/online_retail/data-001/data.csv"
DataPath = userhome + "/delta/customer-data/"

dbutils.fs.rm(DataPath, True)

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

rawDataDF = (spark.read
  .option("header", "true")
  .schema(inputSchema)
  .csv(inputPath)
)

# write to databricks delta 
rawDataDF.write.mode("overwrite").format("delta").partitionBy("Country").save(DataPath)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### CREATE A Table Using Databricks Delta
# MAGIC
# MAGIC Create a table called `customer_data_delta` using `DELTA` out of the above data.
# MAGIC
# MAGIC The notation is:
# MAGIC > `CREATE TABLE <table-name>` <br>
# MAGIC   `USING DELTA` <br>
# MAGIC   `LOCATION <path-do-data> ` <br>
# MAGIC
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Since Delta stores schema (and partition) info in the `_delta_log` directory, we do not have to specify partition columns!

# COMMAND ----------

spark.sql("""
  DROP TABLE IF EXISTS customer_data_delta
""")
spark.sql("""
  CREATE TABLE customer_data_delta
  USING DELTA
  LOCATION '{}'
""".format(DataPath))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Perform a simple `count` query to verify the number of records.
# MAGIC
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Notice how the count is right off the bat; no need to worry about table repairs.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM customer_data_delta

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Metadata
# MAGIC
# MAGIC Since we already have data backing `customer_data_delta` in place,
# MAGIC the table in the Hive metastore automatically inherits the schema, partitioning,
# MAGIC and table properties of the existing data.
# MAGIC
# MAGIC Note that we only store table name, path, database info in the Hive metastore,
# MAGIC the actual schema is stored in the `_delta_log` directory as shown below.

# COMMAND ----------

display(dbutils.fs.ls(DataPath + "/_delta_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC Metadata is displayed through `DESCRIBE DETAIL <tableName>`.
# MAGIC
# MAGIC As long as we have some data in place already for a Databricks Delta table, we can infer schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL customer_data_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC Using Databricks Delta to create tables is quite straightforward and you do not need to specify schemas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Delta Batch Operations - Append

# COMMAND ----------

# MAGIC %md
# MAGIC Set up relevant paths.

# COMMAND ----------

miniDataInputPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-mini.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Here, we add new data to the consumer product data.

# COMMAND ----------

newDataDF = (spark
  .read
  .option("header", "true")
  .schema(inputSchema)
  .csv(miniDataInputPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Do a simple count of number of new items to be added to production data.

# COMMAND ----------

newDataDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## APPEND Using Databricks Delta
# MAGIC
# MAGIC Next, repeat the process by writing to Databricks Delta format.
# MAGIC
# MAGIC In the next cell, load the new data in Databricks Delta format and save to `delta/customer-data/`.

# COMMAND ----------

(newDataDF
  .write
  .format("delta")
  .partitionBy("Country")
  .mode("append")
  .save(DataPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Perform a simple `count` query to verify the number of records and notice it is correct.
# MAGIC
# MAGIC Should be `65535`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM customer_data_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC With Databricks Delta, you can easily append new data without schema-on-read issues.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Delta Batch Operations - Upsert

# COMMAND ----------

# MAGIC %md
# MAGIC Set up relevant paths.

# COMMAND ----------

miniDataPath = userhome + "/delta/customer-data-mini/"
miniDataInputPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-mini.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## UPSERT
# MAGIC
# MAGIC Literally means "UPdate" and "inSERT". It means to atomically either insert a row, or, if the row already exists, UPDATE the row.
# MAGIC
# MAGIC Alter data by changing the values in one of the columns for a specific `CustomerID`.
# MAGIC
# MAGIC Let's load the CSV file `../outdoor-products-mini.csv`.

# COMMAND ----------

miniDataDF = (spark
  .read
  .option("header", "true")
  .schema(inputSchema)
  .csv(miniDataInputPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPSERT Using Non-Databricks Delta Pipeline
# MAGIC
# MAGIC This feature is not supported in non-Delta pipelines.
# MAGIC
# MAGIC To UPSERT means to "UPdate" and "inSERT". In other words, UPSERT is not an atomic operation. It is literally TWO operations.
# MAGIC
# MAGIC Running an UPDATE could invalidate data that is accessed by the subsequent INSERT operation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPSERT Using Databricks Delta Pipeline
# MAGIC
# MAGIC Using Databricks Delta, however, we can do UPSERTS.

# COMMAND ----------

(miniDataDF
  .write
  .mode("overwrite")
  .format("delta")
  .save(miniDataPath)
)

spark.sql("""
    DROP TABLE IF EXISTS customer_data_delta_mini
  """)
spark.sql("""
    CREATE TABLE customer_data_delta_mini
    USING DELTA
    LOCATION '{}'
  """.format(miniDataPath))

# COMMAND ----------

# MAGIC %md
# MAGIC List all rows with `CustomerID=20993`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customer_data_delta_mini WHERE CustomerID=20993

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Form a new DataFrame where `StockCode` is `99999` for `CustomerID=20993`.
# MAGIC
# MAGIC Create a table `customer_data_delta_to_upsert` that contains this data.
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You need to convert `InvoiceNo` to a `String` because Delta infers types and `InvoiceNo` looks like it should be an integer.

# COMMAND ----------

from pyspark.sql.functions import lit, col
customerSpecificDF = (miniDataDF
  .filter("CustomerID=20993")
  .withColumn("StockCode", lit(99999))
  .withColumn("InvoiceNo", col("InvoiceNo").cast("String"))
 )

spark.sql("DROP TABLE IF EXISTS customer_data_delta_to_upsert")
customerSpecificDF.write.saveAsTable("customer_data_delta_to_upsert")

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert the new data into `customer_data_delta_mini`.
# MAGIC
# MAGIC Upsert is done using the `MERGE INTO` syntax.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO customer_data_delta_mini
# MAGIC USING customer_data_delta_to_upsert
# MAGIC ON customer_data_delta_mini.CustomerID = customer_data_delta_to_upsert.CustomerID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     customer_data_delta_mini.StockCode = customer_data_delta_to_upsert.StockCode
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
# MAGIC   VALUES (
# MAGIC     customer_data_delta_to_upsert.InvoiceNo,
# MAGIC     customer_data_delta_to_upsert.StockCode,
# MAGIC     customer_data_delta_to_upsert.Description,
# MAGIC     customer_data_delta_to_upsert.Quantity,
# MAGIC     customer_data_delta_to_upsert.InvoiceDate,
# MAGIC     customer_data_delta_to_upsert.UnitPrice,
# MAGIC     customer_data_delta_to_upsert.CustomerID,
# MAGIC     customer_data_delta_to_upsert.Country)

# COMMAND ----------

# MAGIC %md
# MAGIC Notice how this data is seamlessly incorporated into `customer_data_delta_mini`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customer_data_delta_mini WHERE CustomerID=20993

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC In this Lesson, we used Databricks Delta to UPSERT data into existing Databricks Delta tables.

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
