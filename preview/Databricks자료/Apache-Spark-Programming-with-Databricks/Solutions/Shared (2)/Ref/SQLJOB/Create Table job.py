# Databricks notebook source
df = spark.read.parquet('abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/ODS_CLAIM')
df.write.saveAsTable("tmon_netezza.ODS.ODS_CLAIM",  path='abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/delta/tmon_netezza/ODS/ODS_CLAIM/')

# COMMAND ----------

df = spark.read.parquet('abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/ODS_BUY')
df.write.saveAsTable("tmon_netezza.ODS.ODS_BUY",  path='abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/delta/tmon_netezza/ODS/ODS_BUY/')

# COMMAND ----------

df = spark.read.parquet('abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/ODS_DEAL_MAX/*')
df.write.saveAsTable("tmon_netezza.ODS.ODS_DEAL_MAX",  path='abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/delta/tmon_netezza/ODS/ODS_DEAL_MAX/')

# COMMAND ----------

df = spark.read.parquet('abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/ODS_DEAL_OPTION_EXTRA')
df.write.saveAsTable("tmon_netezza.ODS.ODS_DEAL_OPTION_EXTRA",  path='abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/delta/tmon_netezza/ODS/ODS_DEAL_OPTION_EXTRA/')

# COMMAND ----------

df = spark.read.parquet('abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/ODS_DEAL_OPTION_GROUP')
df.write.saveAsTable("tmon_netezza.ODS.ODS_DEAL_OPTION_GROUP",  path='abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/delta/tmon_netezza/ODS/ODS_DEAL_OPTION_GROUP/')

# COMMAND ----------

df = spark.read.parquet('abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/ODS_DISCOUNT_TARGET_MCS/*')
df.write.saveAsTable("tmon_netezza.ODS.ODS_DISCOUNT_TARGET_MCS",  path='abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/delta/tmon_netezza/ODS/ODS_DISCOUNT_TARGET_MCS/')

# COMMAND ----------

df = spark.read.parquet('abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/ODS_PARTNER_DEAL')
df.write.saveAsTable("tmon_netezza.ODS.ODS_PARTNER_DEAL",  path='abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/delta/tmon_netezza/ODS/ODS_PARTNER_DEAL/')

# COMMAND ----------

df = spark.read.parquet('abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/ODS_PAY')
df.write.saveAsTable("tmon_netezza.ODS.ODS_PAY",  path='abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/delta/tmon_netezza/ODS/ODS_PAY/')

# COMMAND ----------

df = spark.read.parquet('abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/BI_D_PARTNER_INFO')
df.write.saveAsTable("tmon_netezza.MART.BI_D_PARTNER_INFO",  path='abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/delta/tmon_netezza/MART/BI_D_PARTNER_INFO/')

# COMMAND ----------

df = spark.read.parquet('abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/BI_D_DEAL_INFO')
df.write.saveAsTable("tmon_netezza.MART.BI_D_DEAL_INFO",  path='abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/delta/tmon_netezza/MART/BI_D_DEAL_INFO/')
