# Databricks notebook source
# MAGIC
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Modern Data Warehouse Hackathon
# MAGIC
# MAGIC ## Scenario
# MAGIC
# MAGIC Adventureworks is a Bicycle company that sells bicycles worldwide. They work with a partner
# MAGIC to handle their internet sales. Their partner exports the sales history on a monthly basis to Amazon S3
# MAGIC storage. The first challenge is to move data from the Partner's system on AWS into your Azure Environment.
# MAGIC More specifically, you will be moving the data from an S3 bucket to an Azure Blob Storeself.
# MAGIC
# MAGIC This could also be done with Azure Data Factory, but this notebook will walk you through doing the move with
# MAGIC Azure Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Partner Sales Data
# MAGIC
# MAGIC ### Mount the S3 Bucket to the Databricks File System
# MAGIC
# MAGIC Use this code to mount the partner's S3 bucket, the **external mount**, to the Databricks File System.
# MAGIC
# MAGIC Note that we use the mountpoint `/mnt/partnerdata-external`.

# COMMAND ----------

dbutils.fs.unmount("/mnt/partnerdata-external")

# COMMAND ----------

ACCESS_KEY = "AKIAJNZYSCHJVL6KBXCA"
SECRET_KEY = "wDscV3TTRxwNv0OKkMb/UfMMAdiWGpsJ37sPruuq"
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "microsoft-airlift-azure-data-factory-ingestion"
MOUNT_NAME = "partnerdata-external"

try:
  dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)
except Exception as e:
  if "Directory already mounted" in str(e):
    print("Directory already mounted.")
  else:
    raise e
display(dbutils.fs.ls("/mnt/%s" % MOUNT_NAME))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Partner Files
# MAGIC
# MAGIC The format of the partner files is Parquet. Azure Databricks can be used to display the schema of these files.
# MAGIC
# MAGIC #### Order Schema

# COMMAND ----------

orderDate20130508DF = spark.read.parquet("/mnt/partnerdata-external/internetSales/05/OrderDate=2013-05-08/")
orderDate20130508DF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Folder Structure
# MAGIC
# MAGIC The file system utility can be used to display the folder structure. Here, each `OrderDate=2013-05-*` is a parquet file containing the corresponding day's orders.

# COMMAND ----------

# MAGIC %fs ls /mnt/partnerdata-external/internetSales/05

# COMMAND ----------

# MAGIC %md
# MAGIC # Hackathon Challenge
# MAGIC
# MAGIC They would like to use this data to draw meaningful insights. They approached you to build an end to end solution that addresses data ingestion, cleansing, preparation, analysis, and orchestration.
# MAGIC
# MAGIC You need build a Modern Data Warehouse (MDW) solution that meets the following criteria:
# MAGIC 1. Ingest the Parquet data from S3 for the month of May (`05`) into an Azure BLOB container in your Azure subscription.
# MAGIC 1. Perform necessary transformations to the ingested data and load the data into a table named `FactInternetSalesStg` in your SQLDW database.
# MAGIC 1. Load the products dump file into a table named `DimProductStg` in the most efficient way possible.
# MAGIC 1. Write a query to join both tables above to generate a report that looks similar to below:
# MAGIC 1. Create a reporting user named `rptusr` with absolute minimum privileges to execute the query above.
# MAGIC 1. The query observed to be not performing optimally. Investigate the reasons for this performance issue and make necessary changes to the table structure so that query can perform optimally.
# MAGIC  
# MAGIC ## Bonus:
# MAGIC There is data for additional months stored in S3 storage. Develop your Azure infrastructure to ingest this data from each month into it’s corresponding folder in your Azure BLOB storage, Transform and load this data into FactInternetSalesStg table.
# MAGIC
# MAGIC Use the following resources for guidance:
# MAGIC - https://docs.microsoft.com/en-us/azure/sql-data-warehouse/load-data-wideworldimportersdw
# MAGIC - https://docs.microsoft.com/en-us/azure/data-factory/
# MAGIC - https://docs.microsoft.com/en-us/azure/azure-databricks/
# MAGIC - https://docs.microsoft.com/en-us/azure/sql-data-warehouse/sql-data-warehouse-manage-monitor
# MAGIC - https://docs.microsoft.com/en-us/azure/sql-database/sql-database-manage-logins?toc=/azure/sql-data-warehouse/toc.json

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Mount Azure Blob Store
# MAGIC
# MAGIC You should begin by mounting the Azure Blob Store, the **internal mount**, where you intend to store the partner files.

# COMMAND ----------


MOUNTPOINT = "/mnt/partnerdata-internal"

# modify this value to match your Azure Storage Account
STORAGE_ACCOUNT = ""

# modify this value to match your Azure Storage Container
CONTAINER = ""

# modify this value to include the Shared Access Signature Token
SASTOKEN = ""

# Do not change these values
SOURCE = "wasbs://{container}@{storage_acct}.blob.core.windows.net/".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)
URI = "fs.azure.sas.{container}.{storage_acct}.blob.core.windows.net".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)

try:
  dbutils.fs.mount(
    source=SOURCE,
    mount_point=MOUNTPOINT,
    extra_configs={URI:SASTOKEN})
except Exception as e:
  if "Directory already mounted" in str(e):
    pass # Ignore error if already mounted.
  else:
    raise e
print("Success.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Ingest the Parquet data from S3 for the month of May (`05`) into an Azure BLOB container in your Azure subscription.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Perform necessary transformations to the ingested data and load the data into a table named `FactInternetSalesStaging` in your SQLDW database.

# COMMAND ----------

# MAGIC %md
# MAGIC Write your transformed data to the `FactInternetSalesStaging` table in the Data Warehouse using the JDBC connector.
# MAGIC
# MAGIC Don't forget to create this table using a `CREATE TABLE AS` SQL command in the Azure Portal.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
