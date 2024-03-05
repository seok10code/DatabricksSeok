# Databricks notebook source
dbutils.widgets.text("path","")
dbutils.widgets.text("tablename","")
dbutils.widgets.text("crifiled","")

# COMMAND ----------

crifiled=dbutils.widgets.get("crifiled")
tblnm=dbutils.widgets.get("tablename")
spark.sql(f"delete from hansol_paper.mes.{tblnm} where {crifiled} >= concat(date_format(add_months(current_date(), -1), 'yyyyMM'),'01') and {crifiled} < concat(date_format(current_date(), 'yyyyMM'),'01')")

# COMMAND ----------


path=dbutils.widgets.get("path")
tblnm=dbutils.widgets.get("tablename")
df = spark.read.format("parquet").load(f"abfss://mes@datalakeirstvm.dfs.core.windows.net/{path}/*.parquet")
df.write.mode("append").format("delta").saveAsTable(f"hansol_paper.mes.{tblnm}")

