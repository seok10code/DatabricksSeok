# Databricks notebook source
dbutils.widgets.text('date_year', "9999")
dbutils.widgets.text('date_month', '12')
dbutils.widgets.text('date_day', '31')
dbutils.widgets.text('date_hour', '24')

# COMMAND ----------

date_year = dbutils.widgets.get("date_year")
date_month = dbutils.widgets.get("date_month")
date_day = dbutils.widgets.get("date_day")
date_hour = dbutils.widgets.get("date_hour")

# COMMAND ----------

df2 = spark.read.parquet(f"abfss://qoo10@bigdatatmonadls001.dfs.core.windows.net/cloocus_mysql/ods_ticket/{date_year}/{date_month}/{date_day}/{date_hour}/*")
df2.createOrReplaceTempView("merget_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO tmon_netezza.cloocus_test.ods_ticket 
# MAGIC USING merget_table
# MAGIC ON tmon_netezza.cloocus_test.ods_ticket.TICKET_SRL = merget_table.ticket_srl
# MAGIC WHEN MATCHED
# MAGIC   THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %run 
# MAGIC
# MAGIC ./eventhub test
