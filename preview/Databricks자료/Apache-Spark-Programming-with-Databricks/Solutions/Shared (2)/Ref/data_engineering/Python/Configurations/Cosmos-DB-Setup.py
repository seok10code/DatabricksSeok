# Databricks notebook source
# MAGIC
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Configure Databricks - Install Cosmos Library
# MAGIC
# MAGIC
# MAGIC **In this lesson you:**
# MAGIC - Configure Databricks to read from and write to Cosmos DB
# MAGIC   - Create a Database and Collection for a Cosmos Storage Account
# MAGIC   - Retrieve the Read-Write Key
# MAGIC   - Install the necesssary library in your Databricks environment

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create a Database and Collection

# COMMAND ----------

# MAGIC %md
# MAGIC First, access the Azure Portal via the link in Azure Databricks
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/airlift/AAHy9d889ERM6rJd2US1kRiqGCLiHzgmtFsB.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC Access the Cosmos DB Acccount.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/airlift/AAGoKFvyGxJFn67sKpjATLzanhVawAzOeR0B.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC Access the Overview Tab.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/airlift/AAH3ZxCyMuNPEa4Ed29WaahxV1hbuOcw1JQB.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC Add a Collection.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/airlift/AAG_kCwU6xxKZZfaGfjjYXH86qvEWawxI2oB.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC Configure the new Collection.
# MAGIC
# MAGIC 1. Create new Database: `AdventureWorks`
# MAGIC 2. Collection Id: `ratings`
# MAGIC 3. Partition Key: `/ratings`
# MAGIC 4. Throughput: `1000`
# MAGIC 5. Click "OK"
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/airlift/AAE8lbPZL8dNG4-NmEHFm-FodR6tG_nY7EMB.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Retrieve the Read-Write Key

# COMMAND ----------

# MAGIC %md
# MAGIC Access the Keys tab.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/airlift/AAEPzJJpwjdGmJ7UNXjL8CtCNZf2pYCuuu0B.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC Access the Keys tab.
# MAGIC
# MAGIC 1. Retrieve the URI
# MAGIC 2. Retrieve the Primary Key
# MAGIC
# MAGIC You will use both of these in the next notebook.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/airlift/AAEP56yhDa5E3L5nNoI2q9tZbYjlHgXYg_4B.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Install the necesssary library in your Databricks environment

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Right click on the browser tab and select "Duplicate" to open a new tab.
# MAGIC 2. Click on "Azure Databricks" in the upper righthand corder of the screen
# MAGIC 3. Click on "Library" <br>
# MAGIC
# MAGIC <div><img src="https://files.training.databricks.com/courses/azure-databricks/images/MSFT-Cosmos-setup-10.png"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC 1. For "Source" select "Maven Coordinate".
# MAGIC 1. For "Maven Coordinate" copy/paste:
# MAGIC    `com.databricks.training:databricks-cosmosdb-spark2.2.0-scala2.11:1.0.0`
# MAGIC 1. For "Repository" copy/paste
# MAGIC    `https://files.training.databricks.com/repo`
# MAGIC 1. Click "Create Library".

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, attach the library to the Databricks cluster.
# MAGIC
# MAGIC <div><img src="https://www.evernote.com/l/AAEdWohGanRDV6rhOVNK8_1Yo7_n4pqwl2YB/image.png"/></div><br/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
