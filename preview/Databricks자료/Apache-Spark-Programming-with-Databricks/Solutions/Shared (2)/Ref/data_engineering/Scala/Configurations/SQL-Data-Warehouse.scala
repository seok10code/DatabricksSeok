// Databricks notebook source
// MAGIC
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Configure Databricks - Set up Azure SQL Data Warehouse
// MAGIC
// MAGIC
// MAGIC **In this lesson you:**
// MAGIC  - Create a Data Warehouse using the Sample Data

// COMMAND ----------

// MAGIC %md
// MAGIC First, access the Azure Portal via the link in Azure Databricks
// MAGIC
// MAGIC <img src="https://files.training.databricks.com/images/airlift/AAHy9d889ERM6rJd2US1kRiqGCLiHzgmtFsB.png" width=800px />

// COMMAND ----------

// MAGIC %md
// MAGIC Access the SQL Server Acccount.
// MAGIC
// MAGIC <img src="https://www.evernote.com/l/AAER8J6pSRlLRIW-G-3AsOZok-QNS5jrWWQB/image.png" width=800px />

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Access the Overview Tab and 2. add a new data warehouse.
// MAGIC
// MAGIC <img src="https://www.evernote.com/l/AAHUeUHIkxFC-5edvl5Mlp4hYx08Z97PG8QB/image.png" width=800px />

// COMMAND ----------

// MAGIC %md
// MAGIC Configure the new SQL Data Warehouse:
// MAGIC
// MAGIC 1. Name the Database: `Adventure-Works`
// MAGIC 2. Choose "Sample" as source
// MAGIC 3. Choose "AdventureWorksDW" as the Sample
// MAGIC 4. Click "OK"
// MAGIC
// MAGIC This step will take a moment to complete.
// MAGIC
// MAGIC <img src="https://www.evernote.com/l/AAGEv7KfBJpO-qt8Hut0xEwY2o2UWRbHwUAB/image.png" width=800px />

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Configure JDBC Connection to Azure Data Warehouse
// MAGIC
// MAGIC Interfacing with the Data Warehouse requires a connection string. This can be retrieved from the Azure Portal.

// COMMAND ----------

// MAGIC %md
// MAGIC On the control panel for the Data Warehouse display connection strings.
// MAGIC
// MAGIC <img src="https://files.training.databricks.com/images/airlift/AAFexM-WULVLuYWzK5IUYCUBxofLzXHT_Y0B.png" width=800px />

// COMMAND ----------

// MAGIC %md
// MAGIC On the page for connection strings (1) select JDBC, then (2) copy this string to be used below.
// MAGIC
// MAGIC <img src="https://files.training.databricks.com/images/airlift/AAG2kaGJY-dBsowLfURST3tVnZabLMcirgcB.png" width=800px />

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
