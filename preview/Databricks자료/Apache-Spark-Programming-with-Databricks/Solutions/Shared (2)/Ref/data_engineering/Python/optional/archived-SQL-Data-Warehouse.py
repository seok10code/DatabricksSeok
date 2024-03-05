# Databricks notebook source
# MAGIC
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Populating the Azure Data Warehouse
# MAGIC
# MAGIC
# MAGIC **In this lesson you:**
# MAGIC * Use the SQL Data Warehouse Polybase Connector
# MAGIC * Ingest JSON data and write to the Azure Data Warehouse.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %run ./Includes/Database-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Add the connection string with your username and password below.

# COMMAND ----------


JDBC_URL_DATA_WAREHOUSE = ""

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Reading and Transforming JSON data
# MAGIC
# MAGIC   **`/mnt/training-msft/initech/PartnerOrder.json`** contains details about our partner sales.

# COMMAND ----------

# MAGIC %md
# MAGIC The partner reseller writes json files to the Azure Blog Store to report sales.
# MAGIC
# MAGIC Start by reading in the latest sales using a known schema.

# COMMAND ----------

from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType, TimestampType, DecimalType, DoubleType

jsonSchema = StructType([
  StructField("ProductKey", IntegerType(), True),
  StructField("OrderDateKey", IntegerType(), True),
  StructField("DueDateKey", IntegerType(), True),
  StructField("ShipDateKey", IntegerType(), True),
  StructField("ResellerKey", IntegerType(), True),
  StructField("EmployeeKey", IntegerType(), True),
  StructField("PromotionKey", IntegerType(), True),
  StructField("CurrencyKey", IntegerType(), True),
  StructField("SalesTerritoryKey", IntegerType(), True),
  StructField("SalesOrderNumber", StringType(), True),
  StructField("SalesOrderLineNumber", IntegerType(), True),
  StructField("RevisionNumber", IntegerType(), True),
  StructField("OrderQuantity", IntegerType(), True),
  StructField("UnitPrice", DecimalType(19,4), True),
  StructField("ExtendedAmount", DecimalType(19,4), True),
  StructField("UnitPriceDiscountPct", DoubleType(), True),
  StructField("DiscountAmount", DoubleType(), True),
  StructField("ProductStandardCost", DecimalType(19,4), True),
  StructField("TotalProductCost", DecimalType(19,4), True),
  StructField("SalesAmount", DecimalType(19,4), True),
  StructField("TaxAmt", DecimalType(19,4), True),
  StructField("Freight", DecimalType(19,4), True),
  StructField("CarrierTrackingNumber", StringType(), True),
  StructField("CustomerPONumber", StringType(), True),
 ])

# COMMAND ----------


PARTNER_ORDER_FILE = "/mnt/training/initech/PartnerOrderClean.json"

partnerOrdersDF = (spark.read
  .schema(jsonSchema)
  .json(PARTNER_ORDER_FILE)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Ready to update the **`FactResellerSales`** table in the Azure Data Warehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Comparing Connectors
# MAGIC * JDBC
# MAGIC * SQL Data Warehouse Connector

# COMMAND ----------

# MAGIC %md
# MAGIC #### The JDBC Connector approach
# MAGIC Using JDBC (Java's version of ODBC) to write to the SQL Data Warehouse can be very slow when the number of records is large.  A significant performance differnce can be seen even with a relatively small number of records.

# COMMAND ----------


partnerOrdersDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Update the `FactInternetSales` table in the Data Warehouse using the JDBC connector.

# COMMAND ----------

# delete records if they already exist.
ClassroomHelper.sql_update(JDBC_URL_DATA_WAREHOUSE, "DELETE FROM FactResellerSales WHERE ProductKey > 7000")

(partnerOrdersDF.write
  .mode("append")
  .jdbc(JDBC_URL_DATA_WAREHOUSE, "FactResellerSales", mode="append"))

# COMMAND ----------

# MAGIC %md
# MAGIC Running this cell a few times produces an average runtime of approx 26-30 seconds.

# COMMAND ----------

# MAGIC %md
# MAGIC ### The SQL Data Warehouse Polybase Connector approach
# MAGIC The SQL Data Warehouse can read directly from Parquet files when loading into the Data Warehouse.  For very large numbers of records this leads to significant performance improvements.  For more information please see the <a href="https://docs.databricks.com/spark/latest/data-sources/azure/sql-data-warehouse.html" target="_blank">Documentation</a>.
# MAGIC
# MAGIC Note: The SQL DW Connector is availabe on <a href="https://docs.databricks.com/release-notes/runtime/4.0.html" target="_blank">Databricks Runtime 4.0</a> and above.  Be sure the cluster you are attached to is running this version or higher.

# COMMAND ----------

# Blob store for the Polybase SQL Data Warehouse connector's staging area
POLYBASE_BLOB_STORAGE_ACCOUNT="databrickstraining" + ".blob.core.windows.net"
POLYBASE_BLOB_NAME="polybase"
POLYBASE_BLOB_ACCESS_KEY="BXOG8lPEcgSjjlmsOgoPdVCpPDM/RwfN1QTrlXEX3oq0sSbNZmNPyE8By/7l9J1Z7SVa8hsKHc48qBY1tA/mgQ=="
POLYBASE_TEMP_DIR = "wasbs://{blob}@{account}/user/{username}/".format(blob=POLYBASE_BLOB_NAME, account=POLYBASE_BLOB_STORAGE_ACCOUNT, username=username)
sqlContext.sparkSession.conf.set("fs.azure.account.key."+POLYBASE_BLOB_STORAGE_ACCOUNT, POLYBASE_BLOB_ACCESS_KEY)

# COMMAND ----------

# The Data Warehouse requires a "Master Key" that it will use to encrypt any blob store credentials it uses.
# This is something that has to be setup in advance before using the polybase connector.  It is a one-time
# operation.  The code below will create a master key and ignore the error if one has already been created.

# Scala Notebooks will need to define the JDBC_URL_DATA_WAREHOUSE connection string for the Python runtime
# JDBC_URL_DATA_WAREHOUSE = ""

try:
  ClassroomHelper.sql_update(JDBC_URL_DATA_WAREHOUSE, "CREATE MASTER KEY")
  print("Created data warehouse master key.")
except Exception as e:
  if (hasattr(e, "java_exception") and e.java_exception.getMessage() ==
      'There is already a master key in the database. Please drop it before performing this statement.'):
    print("Verified data warehouse master key already exists.")
  else:
    raise e # It's some other error we didn't expect, rethrow it.

# COMMAND ----------

#delete records if they already exist.
ClassroomHelper.sql_update(JDBC_URL_DATA_WAREHOUSE, "DELETE FROM FactResellerSales WHERE ProductKey > 7000")

# Set up the Blob Storage account access key in the notebook session conf.
spark.conf.set("fs.azure.account.key.databrickstraining.blob.core.windows.net", POLYBASE_BLOB_ACCESS_KEY)
spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")

(partnerOrdersDF.write
  .format("com.databricks.spark.sqldw")
  .mode("append")
  .option("url", JDBC_URL_DATA_WAREHOUSE)
  .option("forward_spark_azure_storage_credentials", "true")
  .option("dbtable", "FactResellerSales")
  .option("tempdir", POLYBASE_TEMP_DIR)
  .save())

# COMMAND ----------

# MAGIC %md
# MAGIC The SQL Data Warehouse Connector provides a very siginifcant performance boost, especially on larger datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading from the SQL Data Warehouse

# COMMAND ----------

# Create a Temp View to access using Spark SQL
spark.read.jdbc(JDBC_URL_DATA_WAREHOUSE, "FactResellerSales").createOrReplaceTempView("ResellerSales")

# COMMAND ----------

# Create a new DataFrame that points to data in the Data Warehouse
factResellerSalesDF = spark.read.jdbc(JDBC_URL_DATA_WAREHOUSE, "FactResellerSales").filter("ProductKey > 7000")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
