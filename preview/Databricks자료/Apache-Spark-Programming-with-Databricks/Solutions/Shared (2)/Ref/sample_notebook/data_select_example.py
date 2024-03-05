# Databricks notebook source
dbutils.fs.ls("abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/ODS_DISCOUNT_TARGET_MCS")

# COMMAND ----------

df = spark.read.parquet("abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/ODS_DISCOUNT_TARGET_MCS/600000000")

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView("test")

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(DISCOUNT_TARGET_MCS_SEQNO) from test

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(DISCOUNT_TARGET_MCS_SEQNO) from test

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/ODS_DISCOUNT_TARGET_MCS/600000000`

# COMMAND ----------


