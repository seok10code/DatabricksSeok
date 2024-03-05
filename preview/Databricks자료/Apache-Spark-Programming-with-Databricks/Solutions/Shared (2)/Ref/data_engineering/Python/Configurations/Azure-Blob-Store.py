# Databricks notebook source
# MAGIC
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Mount Blob Store

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC Define your Azure Blob credentials.  You need the following elements:<br><br>
# MAGIC
# MAGIC * Storage account name
# MAGIC * Container name
# MAGIC * Mount point (how the mount will appear in DBFS)
# MAGIC * Shared Access Signature (SAS) key

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC First, access a Storage Account in the Azure Portal.
# MAGIC
# MAGIC <img src="https://www.evernote.com/l/AAGrYLta0I1EhpyE8O8vK5S4JyIsyhQxhPwB/image.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Next, access the Blobs associated with this storage account.
# MAGIC
# MAGIC <img src="https://www.evernote.com/l/AAGb6jRwFNdAEqSBm7eboRfy_QzSbaOoowIB/image.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This page shows both the Storage Account, here `cs2204e9` and the Container Name, here `salesdata`.
# MAGIC
# MAGIC <img src="https://www.evernote.com/l/AAFPLDFwapdMwY9n4GRayGzfGiIdMUG35ecB/image.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC For access to the Blob Store retrieve the Shared Access Signature (SAS) Token.
# MAGIC
# MAGIC Deselect the appropriate permissions to create a "Read-Only" Token. Click "Generate SAS and connection string" to generate the SAS Token.
# MAGIC
# MAGIC <img src="https://www.evernote.com/l/AAGiayNXgRNNJIp4MbzjSZJTh5ZKmqz5mZQB/image.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Retrieve the SAS Token generated.
# MAGIC
# MAGIC <img src="https://www.evernote.com/l/AAFA807DU-pLSJsP84Ao5o1RYDuY72uhctsB/image.png" width=800px />

# COMMAND ----------


# Add the Storage Account, Container, and SAS Token
STORAGE_ACCOUNT = "airlift453"
CONTAINER = "class-453"
SASTOKEN = "?sv=2017-11-09&ss=bfqt&srt=sco&sp=rl&se=2019-12-01T02:53:45Z&st=2018-11-30T18:53:45Z&spr=https&sig=8dG9Emzm2CqocAU0NIwtPtZC492Dr8uW8B9kqLp%2FdzM%3D"

# Do not change these values
SOURCE = "wasbs://{container}@{storage_acct}.blob.core.windows.net/".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)
URI = "fs.azure.sas.{container}.{storage_acct}.blob.core.windows.net".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)
MOUNTPOINT = "/mnt/salesdata4"

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
# MAGIC
# MAGIC Use the Databricks File System to display the contents of the mount.

# COMMAND ----------

# MAGIC %fs ls /mnt/salesdata/adventure-works

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
