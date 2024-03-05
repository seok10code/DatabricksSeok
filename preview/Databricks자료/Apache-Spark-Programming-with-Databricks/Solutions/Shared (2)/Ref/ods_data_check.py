# Databricks notebook source
dbutils.fs.ls("abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/ODS_DISCOUNT_TARGET_MCS")

# COMMAND ----------

dbutils.fs.ls("abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/ODS_TICKET")

# COMMAND ----------

df = spark.read.format('parquet').load('abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/ODS_TICKET/odbc_ADMINODS_LOCAL_CATEGORY.parquet',header=True)

# COMMAND ----------

#df.filter((df['created_at'] == '2023-04-01 12:00:01')).show()
#df.printSchema()
df.limit(100).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select date_format(created_at,'yyyy-MM-dd hh') AS HH, count(DISTINCT main_buy_srl), sum(amount) as ticket_amount 
# MAGIC from parquet.`abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/ODS_TICKET/odbc_ADMINODS_LOCAL_CATEGORY.parquet`
# MAGIC where created_at >= '2023-04-02' and created_at < '2023-04-03'
# MAGIC GROUP BY date_format(created_at,'yyyy-MM-dd hh')
# MAGIC order by date_format(created_at,'yyyy-MM-dd hh')
# MAGIC limit 100
