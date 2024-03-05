# Databricks notebook source
dbutils.widgets.text("path","")
dbutils.widgets.text("tablename","")
dbutils.widgets.text("crifiled","")

# COMMAND ----------

crifiled=dbutils.widgets.get("crifiled")
tblnm=dbutils.widgets.get("tablename")
spark.sql(f"delete from hansol_paper.mes.{tblnm} where {crifiled} >= date_format(date_add(current_date(), -7), 'yyyyMMdd') and {crifiled} < date_format(current_date(), 'yyyyMMdd')")


df = spark.read.format("parquet").load(f"abfss://mes@datalakeirstvm.dfs.core.windows.net/public/{tblnm}/week/*.parquet")
df.write.mode("append").format("delta").saveAsTable(f"hansol_paper.mes.{tblnm}")
