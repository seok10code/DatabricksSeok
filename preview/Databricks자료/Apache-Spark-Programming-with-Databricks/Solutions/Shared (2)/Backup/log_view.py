# Databricks notebook source
import datetime
now_datetime = datetime.datetime.now()
yesterday = now_datetime+ datetime.timedelta(days=-1)+datetime.timedelta(hours=9)
#print('20일 후 :', now_datetime + datetime.timedelta(days=-1))
year = yesterday.strftime('%Y')
month =yesterday.strftime('%m')
day = yesterday.strftime('%d')
print('연도 :', year) ## 연
print('월 :', month) ## 월
print('일 :', day) ## 일


# COMMAND ----------

from pyspark.sql.types import *
scehma = StructType([
StructField("activityName",StringType(),True),
StructField("activityRunId",StringType(),True),
StructField("activityType",StringType(),True),
StructField("category",StringType(),True),
StructField("end",StringType(),True),
StructField("operationName",StringType(),True),
StructField("pipelineName",StringType(),True),
StructField("pipelineRunId",StringType(),True),
StructField("properties",StructType([
    StructField("Input",StructType([
        StructField("source",StructType([
            StructField("type",StringType(),True),
            StructField("query",StringType(),True)
        ]),True),
        StructField("sink",StructType([
            StructField("type",StringType(),True)
        ]),True)
    ]),True),
    StructField("Output",StructType([
        StructField("dataRead",StringType(),True),
        StructField("dataWritten",StringType(),True),
        StructField("filesWritten",StringType(),True),
        StructField("rowsCopied",StringType(),True),
        StructField("rowsRead",StringType(),True),
        StructField("copyDuration",StringType(),True)
    ]),True),
    StructField("Error",StructType([
        StructField("errorCode",StringType(),True),
        StructField("message",StringType(),True),
        StructField("failureType",StringType(),True)
    ]),True)
]),True),
StructField("start",StringType(),True),
StructField("status",StringType(),True),
StructField("tags",StringType(),True),
StructField("time",StringType(),True),
StructField("timestamp",StringType(),True)
])

# COMMAND ----------

df = spark.read.schema(scehma).json(f"abfss://insights-logs-activityruns@datalakeirstvm.dfs.core.windows.net/resourceId=/SUBSCRIPTIONS/66A3B7E6-CB1D-4411-A5DF-0E6037E11853/RESOURCEGROUPS/PAPER-DATALAKE/PROVIDERS/MICROSOFT.DATAFACTORY/FACTORIES/PAP-DATAFACTORY1/y={year}/m={month}/d={day}/*/*/*.json")
df.write.mode('append').saveAsTable("hansol_paper.managed.activity_log_raw")

# COMMAND ----------

## 2023-12-19 start,end컬럼 변경 start -> CAST(start AS TIMESTAMP) + INTERVAL 9 HOURS

silver = spark.sql(f"""
select activityName
, activityType, category, pipelineName, 
properties.Input.source.type as source_type,
properties.Input.source.query as source_query, 
properties.Input.sink.type as sink_type, 
properties.Output.dataRead as Output_dataRead,
properties.Output.dataWritten as Output_dataWritten,
properties.Output.filesWritten as Output_filesWritten,
properties.Output.rowsCopied as Output_rowsCopied,
properties.Output.rowsRead as Output_rowsRead,
properties.Output.copyDuration as Output_copyDuration,
properties.Error.errorCode as errorCode,
properties.Error.message as errorMessage,
properties.Error.failureType as errorFailureType,
status,
CAST(CAST(start AS TIMESTAMP) + INTERVAL 9 HOURS AS STRING) as startTime,
CAST(CAST(end AS TIMESTAMP) + INTERVAL 9 HOURS AS STRING) as endTime,
time,
timestamp,
tags,
operationName,
pipelineRunId,
activityRunId
from hansol_paper.managed.activity_log_raw
where status not in ('Queued', 'InProgress')
and cast(timestamp as date) = '{year}-{month}-{day}'
""")
#display(silver)
silver.write.mode('append').saveAsTable("hansol_paper.managed.activity_log_bronze")

# COMMAND ----------

display(silver)
