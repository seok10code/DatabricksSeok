# Databricks notebook source
dbutils.widgets.text("year","")
dbutils.widgets.text("month","")
dbutils.widgets.text("day","")
dbutils.widgets.text("hour","")

# COMMAND ----------


year=dbutils.widgets.get("year")
month=dbutils.widgets.get("month")
day=dbutils.widgets.get("day")
hour=dbutils.widgets.get("hour")
shour = int(hour)-1

date_tm = year+"-"+month+"-"+day+" "+str(shour)+":00:00.000000+09"
spark.conf.set('var.date_tm',date_tm)

spark.sql("""delete from hansol_paper.ipis.paperpi_data where enroll_date >= '${var.date_tm}';""")

df = spark.read.format("parquet").load(f"abfss://ipis@datalakeirstvm.dfs.core.windows.net/public/paperpi_data/yyyy={year}/MM={month}/dd={day}/HH={hour}/*.parquet")
df.write.mode("append").format("delta").saveAsTable(f"hansol_paper.ipis.paperpi_data")

