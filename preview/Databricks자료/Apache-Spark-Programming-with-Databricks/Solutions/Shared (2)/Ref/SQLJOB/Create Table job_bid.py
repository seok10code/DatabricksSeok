# Databricks notebook source
df = spark.read.parquet('abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/BI_D_DEAL_PARTITION/*/*')

# COMMAND ----------

df.write.saveAsTable("tmon_netezza.MART.BI_D_DEAL",  path='abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/delta/tmon_netezza/MART/BI_D_DEAL/')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from tmon_netezza.MART.BI_D_DEAL
