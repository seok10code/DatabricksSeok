-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import from_json, col
-- MAGIC from pyspark.sql.types import *
-- MAGIC schema = StructType([
-- MAGIC   StructField("correlationId", StringType()), 
-- MAGIC   StructField("activityName", StringType()),
-- MAGIC   StructField("pipelineName", StringType()),
-- MAGIC   #StructField("properties", StringType()),
-- MAGIC   StructField("properties", StructType([
-- MAGIC     StructField("Output", StructType([
-- MAGIC         StructField("dataRead", StringType()),
-- MAGIC         StructField("dataWritten", StringType()),
-- MAGIC         StructField("filesWritten", StringType()),
-- MAGIC         StructField("rowsRead", StringType()),
-- MAGIC         StructField("rowsCopied", StringType()),
-- MAGIC         StructField("copyDuration", StringType()),
-- MAGIC         StructField("throughput", StringType()),
-- MAGIC         StructField("errors", ArrayType(StringType(), False)),
-- MAGIC         StructField("executionDetails", ArrayType(StructType([
-- MAGIC             StructField("source", StringType()),
-- MAGIC             StructField("sink", StringType()),
-- MAGIC             StructField("status", StringType()),
-- MAGIC             StructField("start", StringType())
-- MAGIC         ]), False))
-- MAGIC     ]))
-- MAGIC     ]))
-- MAGIC
-- MAGIC ])
-- MAGIC df = spark.read.option("recursiveFileLookup", "true").schema(schema).json("abfss://insights-logs-activityruns@bigdatatmonadls001.dfs.core.windows.net/resourceId=/SUBSCRIPTIONS/483100C7-E0EA-4B08-910B-3613A56F5D5C/RESOURCEGROUPS/BIGDATA_BIGDATAPLATFORM_RG/PROVIDERS/MICROSOFT.DATAFACTORY/FACTORIES/BIGDATA-TMON-ADF-001/")
-- MAGIC df.createTempView('adf_log_test')

-- COMMAND ----------

select activityName, pipelineName, properties.Output.dataRead, properties.Output.dataWritten, properties.Output.filesWritten, properties.Output.rowsRead, properties.Output.rowsCopied, properties.Output.copyDuration, properties.Output.throughput, properties.Output.errors, properties.Output.executionDetails[0].status, properties.Output.executionDetails[0].start from adf_log_test

-- COMMAND ----------

describe adf_log_test

-- COMMAND ----------


