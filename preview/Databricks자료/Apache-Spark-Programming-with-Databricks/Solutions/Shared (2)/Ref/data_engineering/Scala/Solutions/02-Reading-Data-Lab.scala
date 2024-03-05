// Databricks notebook source
// MAGIC
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Reading Data Lab
// MAGIC **Goal:** Read data with Apache Spark.
// MAGIC * Instructions are provided below along with empty cells for you to do your work.
// MAGIC * Additional cells are provided at the bottom of this notebook to verify your work is accurate.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Instructions
// MAGIC 0. Start with the file **dbfs:/mnt/training/initech/products/dirty.csv**, a file containing product details.
// MAGIC 0. Read the data and assign it to a `DataFrame` named **newProductDF**.
// MAGIC 0. Run the last cell to verify the data was loaded correctly and to print its schema.
// MAGIC
// MAGIC Bonus: Create the `DataFrame` and print its schema **without** executing a single job.

// COMMAND ----------

// MAGIC %md
// MAGIC First, run the following cell to verify `/mnt/training` is properly configured.

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Show Your Work

// COMMAND ----------

// ANSWER

val CSV_FILE = "dbfs:/mnt/training/initech/products/dirty.csv"

val newProductDF = spark.read     // The DataFrameReader
  .option("header", true)         // Use first line of all files as header
  .option("inferSchema", true)    // Automatically infer data types
  .csv(CSV_FILE)                   // Creates a DataFrame from CSV after reading in the file

newProductDF.printSchema()

// COMMAND ----------

// ANSWER-Bonus

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

val CSV_FILE = "dbfs:/mnt/training/initech/products/dirty.csv"

val csvSchema = StructType(
  StructField("product_id", IntegerType) ::
  StructField("category", StringType) ::
  StructField("brand", StringType) ::
  StructField("model", StringType) ::
  StructField("price", DoubleType) ::
  StructField("processor", StringType) ::
  StructField("size", StringType) ::
  StructField("display", StringType) :: Nil)

val productDF = spark.read
 .option("header", "true")
 .schema(csvSchema)          // Use the specified schema
 .csv(CSV_FILE)

newProductDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Verify Your Work
// MAGIC Run the following cell to verify the `DataFrame` was created properly.

// COMMAND ----------


val columns = newProductDF.dtypes
columns(1)._2

// COMMAND ----------

// TEST - Run this cell to test your solution.

newProductDF.printSchema()

val columns = newProductDF.dtypes
assert (columns.length == 8, s"Expected 8 columns but found ${columns.length}")

assert (columns(0)._1 == "product_id",  s"""Expected column 0 to be "product_id" but found "${columns(0)._1}".""")
assert (columns(0)._2 == "IntegerType", s"""Expected column 0 to be of type "IntegerType" but found "${columns(0)._2}".""")

assert (columns(1)._1 == "category",    s"""Expected column 1 to be \"category\" but found "${columns(1)._1}".""")
assert (columns(1)._2 == "StringType",  s"""Expected column 1 to be of type \"StringType\" but found "${columns(1)._2}".""")

assert (columns(2)._1 == "brand",       s"""Expected column 2 to be \"brand\" but found "${columns(2)._1}".""")
assert (columns(2)._2 == "StringType",  s"""Expected column 2 to be of type \"StringType\" but found "${columns(2)._2}".""")

assert (columns(3)._1 == "model",       s"""Expected column 3 to be \"model\" but found "${columns(3)._1}".""")
assert (columns(3)._2 == "StringType",  s"""Expected column 3 to be of type \"StringType\" but found "${columns(3)._2}".""")

assert (columns(4)._1 == "price",       s"""Expected column 4 to be \"price\" but found "${columns(4)._1}".""")
assert (columns(4)._2 == "DoubleType",  s"""Expected column 4 to be of type \"DoubleType\" but found "${columns(4)._2}".""")

assert (columns(5)._1 == "processor",   s"""Expected column 5 to be \"processor\" but found "${columns(5)._1}".""")
assert (columns(5)._2 == "StringType",  s"""Expected column 5 to be of type \"StringType\" but found "${columns(5)._2}".""")

assert (columns(6)._1 == "size",        s"""Expected column 6 to be \"size\" but found "${columns(6)._1}".""")
assert (columns(6)._2 == "StringType",  s"""Expected column 6 to be of type \"StringType\" but found "${columns(6)._2}".""")

assert (columns(7)._1 == "display",     s"""Expected column 7 to be \"display\" but found "${columns(7)._1}".""")
assert (columns(7)._2 == "StringType",  s"""Expected column 7 to be of type \"StringType\" but found "${columns(7)._2}".""")

println("Congratulations, all tests passed... that is if no jobs were triggered :-)\n")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
