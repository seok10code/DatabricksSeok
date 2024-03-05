# Databricks notebook source
# MAGIC %md
# MAGIC # 설정방법
# MAGIC  : Azure Data Factory에서 Pipleline 또는 Activity Log를 JSON 파일 형태로 Azure Blob Storage에 저장하는 방법
# MAGIC - Data Factory > [모니터링]진단 설정 > 진단설정추가 또는 설정편집
# MAGIC  * 현재 Pipeline activity runs log, Pipeline runs log 2개만 설정
# MAGIC  * KST가 아닌 UTC 시간 기준으로 스토리지 계정에 JSON 파일 생성 (ex) h=01/m=00/PT1H.json는 실제 KST 기준 오전10~11시에 수행된 작업 로그
# MAGIC

# COMMAND ----------

import datetime
now_datetime_utc = datetime.datetime.now()
now_datetime_kst = now_datetime_utc+datetime.timedelta(hours=9)
year = now_datetime_utc.strftime('%Y')
month =now_datetime_utc.strftime('%m')
day = now_datetime_utc.strftime('%d')
print('UTC Time :', now_datetime_utc)
print('KST Time :', now_datetime_kst)
print('연도 :', year) ## 연
print('월 :', month) ## 월
print('일 :', day) ## 일

# COMMAND ----------

# MAGIC %md
# MAGIC # 당일 데이터 삭제

# COMMAND ----------

spark.sql(f"""delete from hansol_paper.managed.adf_activity_log_bronze t where starttime_UTC like '{year}-{month}-{day}%'""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Azure Data Factory Activity Log until today

# COMMAND ----------

from pyspark.sql.types import *
activity_log_schema = StructType([
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

df_activity_log_raw = spark.read.schema(activity_log_schema).json(f"abfss://insights-logs-activityruns@datalakeirstvm.dfs.core.windows.net/resourceId=/SUBSCRIPTIONS/66A3B7E6-CB1D-4411-A5DF-0E6037E11853/RESOURCEGROUPS/PAPER-DATALAKE/PROVIDERS/MICROSOFT.DATAFACTORY/FACTORIES/PAP-DATAFACTORY1/y={year}/m={month}/d={day}/*/*/*.json")

# COMMAND ----------

df_activity_log_raw.createOrReplaceTempView("vw_df_activity_log_raw")

# COMMAND ----------

df_adf_activity_log_bronze = spark.sql(f"""
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
CAST(start AS TIMESTAMP) + INTERVAL 9 HOURS as startTime_KST,
CAST(end AS TIMESTAMP) + INTERVAL 9 HOURS as endTime_KST,
CAST(time AS TIMESTAMP) + INTERVAL 9 HOURS as time_KST,
CAST(timestamp AS TIMESTAMP) + INTERVAL 9 HOURS as timestamp_KST
, start     as startTime_UTC
, end       as endtTime_UTC
, time      as time_UTC
, timestamp as timestamp_UTC
, tags,
operationName,
pipelineRunId,
activityRunId
from vw_df_activity_log_raw
where status not in ('Queued', 'InProgress')
  and start like '{year}-{month}-{day}%'
""")

# COMMAND ----------

df_adf_activity_log_bronze.write.mode('append').option("mergeSchema", "true").saveAsTable("hansol_paper.managed.adf_activity_log_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Azure Data Factory Pipeline Log until today

# COMMAND ----------

from pyspark.sql.functions import col, explode

# COMMAND ----------

# MAGIC %md
# MAGIC ### 당일 데이터 삭제

# COMMAND ----------

spark.sql(f"""delete from hansol_paper.managed.adf_pipeline_log_bronze t where starttime_UTC like '{year}-{month}-{day}%'""")

# COMMAND ----------

df_pipeline_log_raw_file_path = f"abfss://insights-logs-pipelineruns@datalakeirstvm.dfs.core.windows.net/resourceId=/SUBSCRIPTIONS/66A3B7E6-CB1D-4411-A5DF-0E6037E11853/RESOURCEGROUPS/PAPER-DATALAKE/PROVIDERS/MICROSOFT.DATAFACTORY/FACTORIES/PAP-DATAFACTORY1/y={year}/m={month}/d={day}/*/*/*.json"

# COMMAND ----------

df_pipeline_log_raw = spark.read.json(df_pipeline_log_raw_file_path)

# COMMAND ----------

df_pipeline_log_raw.createOrReplaceTempView("vw_df_pipeline_log_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ### pipelineruns Log - Succeeded 인 경우

# COMMAND ----------

df_pipeline_log_raw_success = spark.sql(f"""select * from vw_df_pipeline_log_raw where status in ('Succeeded') and start like '{year}-{month}-{day}%'""")

# COMMAND ----------

df_flattened_pipeline_log_raw_success = df_pipeline_log_raw_success.select(
  col("EventName")
, col("category")
, col("correlationId")
, col("end")
, col("env_dt_spanId")
, col("env_dt_traceId")
, col("env_name")
, col("env_time")
, col("env_ver")
, col("groupId")
, col("level")
, col("location")
, col("name")
, col("operationName")
, col("pipelineName")
, col("resourceId")
, col("runId")
, col("severityNumber")
, col("severityText")
, col("start")
, col("status")
, col("tags")
, col("time")
, col("timestamp")
, col("properties.Annotations")
, col("properties.Message")
, col("properties.Predecessors.Id")[0].alias("properties.Predecessors.Id")
, col("properties.Predecessors.InvokedByType")[0].alias("properties.Predecessors.InvokedByType")
, col("properties.Predecessors.Name")[0].alias("properties.Predecessors.Name")
, col("properties.SystemParameters.ExecutionStart").alias("properties.SystemParameters.ExecutionStart")
, col("properties.SystemParameters.PipelineRunRequestTime").alias("properties.SystemParameters.PipelineRunRequestTime")
, col("properties.SystemParameters.SubscriptionId").alias("properties.SystemParameters.SubscriptionId")
, col("properties.SystemParameters.TriggerId").alias("properties.SystemParameters.TriggerId")    
)

# COMMAND ----------

df_flattened_pipeline_log_raw_success.createOrReplaceTempView("vw_df_flattened_pipeline_log_raw_success")

# COMMAND ----------

bronze_success = spark.sql(f"""
select   
  `EventName`
, `category`
, `correlationId`
, CAST(`end` AS TIMESTAMP) + INTERVAL 9 HOURS as `endTime_KST`
, CAST(`env_time` AS TIMESTAMP) + INTERVAL 9 HOURS as `env_time_KST`
, CAST(`start` AS TIMESTAMP) + INTERVAL 9 HOURS as `startTime_KST`
, CAST(`time` AS TIMESTAMP) + INTERVAL 9 HOURS as `time_KST`
, CAST(`timestamp` AS TIMESTAMP) + INTERVAL 9 HOURS as `timestamp_KST`
, CAST(`properties.SystemParameters.ExecutionStart` AS TIMESTAMP) + INTERVAL 9 HOURS as `properties.SystemParameters.ExecutionStart_KST`
, CAST(`properties.SystemParameters.PipelineRunRequestTime` AS TIMESTAMP) + INTERVAL 9 HOURS as `properties.SystemParameters.PipelineRunRequestTime_KST`
, `end` as `endTime_UTC`
, `env_time` as `env_time_UTC`
, `start` as `startTime_UTC`
, `time` as `time_UTC`
, `timestamp` as `timestamp_UTC`
, `properties.SystemParameters.ExecutionStart` as `properties.SystemParameters.ExecutionStart_UTC`
, `properties.SystemParameters.PipelineRunRequestTime` as `properties.SystemParameters.PipelineRunRequestTime_UTC`
, `env_dt_spanId`
, `env_dt_traceId`
, `env_name`
, `env_ver`
, null as `failureType`
, `groupId`
, `level`
, `location`
, `name`
, `operationName`
, `pipelineName`
, `resourceId`
, `runId` as pipelineRunId
, `severityNumber`
, `severityText`
, `status`
, `tags`
, `Annotations`
, `Message`
, `properties.Predecessors.Id`
, `properties.Predecessors.InvokedByType`
, `properties.Predecessors.Name`
, `properties.SystemParameters.SubscriptionId`
, `properties.SystemParameters.TriggerId`
from vw_df_flattened_pipeline_log_raw_success
""")

bronze_success.write.mode('append').option("mergeSchema", "true").saveAsTable("hansol_paper.managed.adf_pipeline_log_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ### pipelineruns Log - Failed 인 경우

# COMMAND ----------

df_pipeline_log_raw_fail = spark.sql(f"""select * from vw_df_pipeline_log_raw where status in ('Failed') and start like '{year}-{month}-{day}%'""")

# COMMAND ----------

if not df_pipeline_log_raw_fail.isEmpty():
  df_flattened_pipeline_log_raw_fail = df_pipeline_log_raw_fail.select(
    col("EventName")
  , col("category")
  , col("correlationId")
  , col("end")
  , col("env_dt_spanId")
  , col("env_dt_traceId")
  , col("env_name")
  , col("env_time")
  , col("env_ver")
  , col("failureType") # only failed
  , col("groupId")
  , col("level")
  , col("location")
  , col("name")
  , col("operationName")
  , col("pipelineName")
  , col("resourceId")
  , col("runId")
  , col("severityNumber")
  , col("severityText")
  , col("start")
  , col("status")
  , col("tags")
  , col("time")
  , col("timestamp")
  , col("properties.Annotations")
  , col("properties.Message")
  , col("properties.Predecessors.Id")[0].alias("properties.Predecessors.Id")
  , col("properties.Predecessors.InvokedByType")[0].alias("properties.Predecessors.InvokedByType")
  , col("properties.Predecessors.Name")[0].alias("properties.Predecessors.Name")
  , col("properties.SystemParameters.ExecutionStart").alias("properties.SystemParameters.ExecutionStart")
  , col("properties.SystemParameters.PipelineRunRequestTime").alias("properties.SystemParameters.PipelineRunRequestTime")
  , col("properties.SystemParameters.SubscriptionId").alias("properties.SystemParameters.SubscriptionId")
  , col("properties.SystemParameters.TriggerId").alias("properties.SystemParameters.TriggerId")    
  )

  df_flattened_pipeline_log_raw_fail.createOrReplaceTempView("vw_df_flattened_pipeline_log_raw_fail")

  bronze_fail = spark.sql(f"""
select   
  `EventName`
, `category`
, `correlationId`
, CAST(`end` AS TIMESTAMP) + INTERVAL 9 HOURS as `endTime_KST`
, CAST(`env_time` AS TIMESTAMP) + INTERVAL 9 HOURS as `env_time_KST`
, CAST(`start` AS TIMESTAMP) + INTERVAL 9 HOURS as `startTime_KST`
, CAST(`time` AS TIMESTAMP) + INTERVAL 9 HOURS as `time_KST`
, CAST(`timestamp` AS TIMESTAMP) + INTERVAL 9 HOURS as `timestamp_KST`
, CAST(`properties.SystemParameters.ExecutionStart` AS TIMESTAMP) + INTERVAL 9 HOURS as `properties.SystemParameters.ExecutionStart_KST`
, CAST(`properties.SystemParameters.PipelineRunRequestTime` AS TIMESTAMP) + INTERVAL 9 HOURS as `properties.SystemParameters.PipelineRunRequestTime_KST`
, `end` as `endTime_UTC`
, `env_time` as `env_time_UTC`
, `start` as `startTime_UTC`
, `time` as `time_UTC`
, `timestamp` as `timestamp_UTC`
, `properties.SystemParameters.ExecutionStart` as `properties.SystemParameters.ExecutionStart_UTC`
, `properties.SystemParameters.PipelineRunRequestTime` as `properties.SystemParameters.PipelineRunRequestTime_UTC`
, `env_dt_spanId`
, `env_dt_traceId`
, `env_name`
, `env_ver`
, `failureType`
, `groupId`
, `level`
, `location`
, `name`
, `operationName`
, `pipelineName`
, `resourceId`
, `runId` as pipelineRunId
, `severityNumber`
, `severityText`
, `status`
, `tags`
, `Annotations`
, `Message`
, `properties.Predecessors.Id`
, `properties.Predecessors.InvokedByType`
, `properties.Predecessors.Name`
, `properties.SystemParameters.SubscriptionId`
, `properties.SystemParameters.TriggerId`
from vw_df_flattened_pipeline_log_raw_fail
""")

  bronze_fail.write.mode('append').option("mergeSchema", "true").saveAsTable("hansol_paper.managed.adf_pipeline_log_bronze")


