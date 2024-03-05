# Databricks notebook source
dbutils.fs.ls("abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/delta/tmon_netezza/MART/BI_M_TICKET_NOW_DEAL_PROFIT_DAILY")

# COMMAND ----------

df =  spark.read.parquet("abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/dw_test/data_a7891996-10aa-4004-a702-ca5ab1169d25_8008a93a-9e58-4a50-9b4c-c6cbdbcfc591_00000.parquet", path='')
display(df)

# COMMAND ----------

df.write.saveAsTable("hive_metastore.default.dw_test_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from hive_metastore.default.dw_test_table

# COMMAND ----------

df2 =  spark.read.parquet("abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/dw_test/data_a7891996-10aa-4004-a702-ca5ab1169d25_8008a93a-9e58-4a50-9b4c-c6cbdbcfc591_00001.parquet")

# COMMAND ----------

df.write.mode('append').saveAsTable("hive_metastore.default.dw_test_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from hive_metastore.default.dw_test_table
