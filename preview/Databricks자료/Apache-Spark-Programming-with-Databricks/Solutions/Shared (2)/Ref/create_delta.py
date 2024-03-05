# Databricks notebook source
dbutils.fs.ls("abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/delta/tmon_netezza/MART")

# COMMAND ----------

df = spark.read.parquet("abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/BI_M_TICKET_NOW_DEAL_PROFIT_DAILY/*")
df.write.saveAsTable("tmon_netezza.mart.BI_M_TICKET_NOW_DEAL_PROFIT_DAILY", path="abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/delta/tmon_netezza/MART/BI_M_TICKET_NOW_DEAL_PROFIT_DAILY")

# COMMAND ----------

df = spark.read.parquet("abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/BI_M_TICKET_NOW_DEAL_PROFIT_DAILY_V2/*")
df.write.saveAsTable("tmon_netezza.mart.BI_M_TICKET_NOW_DEAL_PROFIT_DAILY", path="abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/delta/tmon_netezza/MART/BI_M_TICKET_NOW_DEAL_PROFIT_DAILY_V2_TEMP")
