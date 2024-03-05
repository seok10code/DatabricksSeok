// Databricks notebook source
// MAGIC
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Reading and writing from CosmosDB
// MAGIC
// MAGIC **In this lesson you:**
// MAGIC - Write data into Cosmos DB
// MAGIC - Read data from Cosmos DB

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Load Cosmos DB
// MAGIC
// MAGIC Now load a small amount of data into Cosmos to demonstrate that connection

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

// MAGIC %md
// MAGIC <span>1.</span> Enter the CosmosDB connection information into the cell below. <br>

// COMMAND ----------

// TODO
// Fill out the following variables with your information

val PrimaryReadWriteKey = ""
val Endpoint = ""
val CosmosDatabase =  ""
val CosmosCollection = ""

// Current version of the connector
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config

// Configure connection to your collection
val cosmosConfig = Config(Map(
  "Endpoint" -> Endpoint,
  "Masterkey" -> PrimaryReadWriteKey,
  "Database" -> CosmosDatabase,
  "Collection" -> CosmosCollection
))

// COMMAND ----------

// MAGIC %md
// MAGIC <span>2.</span> Read the input parquet file.

// COMMAND ----------

val ratingsDF = (spark.read
  .parquet("/mnt/training/initech/ratings/ratings.parquet/")
  .withColumn("rating", $"rating".cast("double")))
println("Num Rows: " + ratingsDF.count)

// COMMAND ----------

display(ratingsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC <span>3.</span> Write the data to Cosmos DB.

// COMMAND ----------


val ratingsSampleDF = ratingsDF.sample(.001)

ratingsSampleDF.write.mode("overwrite").cosmosDB(cosmosConfig)

// COMMAND ----------

// MAGIC %md
// MAGIC <span>4.</span> Confirm that your data is now in Cosmos DB.

// COMMAND ----------


val dfCosmos = spark.read.cosmosDB(cosmosConfig)

display(dfCosmos)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
