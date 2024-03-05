# Databricks notebook source
# MAGIC %md
# MAGIC ### Azure Data Factory에서 Pipleline 또는 Activity Log를 JSON 파일 형태로 Azure Blob Storage에 저장하는 방법
# MAGIC - Data Factory > [모니터링]진단 설정 > 진단설정추가 또는 설정편집
# MAGIC  => 현재 [pipelinelogtest]라는 이름으로 생성
# MAGIC  * 로그 범주는 현재 Pipeline activity runs log, Pipeline runs log 2개만 설정
# MAGIC  * KST가 아닌 UTC 시간 기준으로 스토리지 계정에 JSON 파일 생성 (ex) h=01/m=00/PT1H.json는 실제 KST 기준 오전10~11시에 수행된 작업 로그

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Azure Data Factory All Activity Log

# COMMAND ----------

# DBTITLE 1,기존 테이블 및 뷰 삭제
# MAGIC %sql
# MAGIC drop view if exists hansol_paper.managed.vw_adf_activity_log_monitor ;
# MAGIC drop table if exists hansol_paper.managed.adf_activity_log_bronze ;

# COMMAND ----------

# DBTITLE 1,JSON 파일 스키마 Configure(Customizing 가능)
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

# DBTITLE 1,BLOB Storage에 저장되어 있는 모든 JSON 로그 파일 dataframe화(위에서 정의된 스키마 구조대로)
df_activity_log_raw = spark.read.schema(activity_log_schema).json(f"abfss://insights-logs-activityruns@datalakeirstvm.dfs.core.windows.net/resourceId=/SUBSCRIPTIONS/66A3B7E6-CB1D-4411-A5DF-0E6037E11853/RESOURCEGROUPS/PAPER-DATALAKE/PROVIDERS/MICROSOFT.DATAFACTORY/FACTORIES/PAP-DATAFACTORY1/*/*/*/*/*/*.json")

# COMMAND ----------

# DBTITLE 1,Temp View 생성
df_activity_log_raw.createOrReplaceTempView("vw_df_activity_log_raw")

# COMMAND ----------

# DBTITLE 1,bronze table에 저장할 데이터의 컬럼 가공 및 필터링
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
""")

# COMMAND ----------

# DBTITLE 1,Bronze Table 생성
df_adf_activity_log_bronze.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable("hansol_paper.managed.adf_activity_log_bronze")

# COMMAND ----------

# DBTITLE 1,모니터용 뷰 생성
# MAGIC %sql
# MAGIC create or replace view hansol_paper.managed.vw_adf_activity_log_monitor 
# MAGIC comment 'ADF(Azure Data Factory) Activity Log Monitor View'
# MAGIC as select 
# MAGIC        tb1.activityName
# MAGIC      , tb1.status       
# MAGIC      , tb1.activityType
# MAGIC      , DATE_FORMAT(tb1.startTime, 'MM/dd/yyyy, hh:mm:ss a') AS `startTime`
# MAGIC      , DATE_FORMAT(tb1.endTime, 'MM/dd/yyyy, hh:mm:ss a')   AS `endTime`
# MAGIC      , lpad(FLOOR(cast(time_diff as int) / 3600),2,'0') || ':' || lpad(FLOOR((cast(time_diff as int) % 3600) / 60),2,'0')  || ':' || lpad(cast(time_diff as int) % 60,2,'0') as `duration`
# MAGIC      , tb1.activityRunId
# MAGIC      , tb1.pipelineRunId
# MAGIC   from (
# MAGIC select t.pipelineName
# MAGIC      , t.activityName
# MAGIC      , t.startTime_KST as startTime
# MAGIC      , t.endTime_KST   as endTime
# MAGIC      , t.pipelineRunId
# MAGIC      , t.activityRunId
# MAGIC      , t.activityType
# MAGIC      , t.Output_copyDuration
# MAGIC      , unix_timestamp(CAST(endTime_KST AS TIMESTAMP)) - unix_timestamp(CAST(startTime_KST AS TIMESTAMP)) AS time_diff
# MAGIC      , t.status
# MAGIC   from hansol_paper.managed.adf_activity_log_bronze t
# MAGIC   order by `startTime` desc
# MAGIC  ) tb1
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Azure Data Factory All Pipeline Log

# COMMAND ----------

# DBTITLE 1,기존 테이블 및 뷰 삭제
# MAGIC %sql
# MAGIC drop view if exists hansol_paper.managed.vw_adf_pipeline_log_monitor ;
# MAGIC drop table if exists hansol_paper.managed.adf_pipeline_log_bronze ;

# COMMAND ----------

from pyspark.sql.functions import col, explode

# COMMAND ----------

# DBTITLE 1,BLOB Storage에 저장되어 있는 모든 pipline log JSON 파일 경로 지정
df_pipeline_log_raw_file_path = f"abfss://insights-logs-pipelineruns@datalakeirstvm.dfs.core.windows.net/resourceId=/SUBSCRIPTIONS/66A3B7E6-CB1D-4411-A5DF-0E6037E11853/RESOURCEGROUPS/PAPER-DATALAKE/PROVIDERS/MICROSOFT.DATAFACTORY/FACTORIES/PAP-DATAFACTORY1/*/*/*/*/*/*.json"

# COMMAND ----------

# DBTITLE 1,JSON 파일 dataframe화
df_pipeline_log_raw = spark.read.json(df_pipeline_log_raw_file_path)

# COMMAND ----------

# DBTITLE 1,Temp View 생성
df_pipeline_log_raw.createOrReplaceTempView("vw_df_pipeline_log_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ### pipelineruns Log - Succeeded 인 경우

# COMMAND ----------

# DBTITLE 1,작업 성공시 로그 필터링
df_pipeline_log_raw_success = spark.sql("select * from vw_df_pipeline_log_raw where status in ('Succeeded')")

# COMMAND ----------

# DBTITLE 1,Bronze 테이블에 적재할 컬럼만 필터링
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

# DBTITLE 1,Temp View 생성
df_flattened_pipeline_log_raw_success.createOrReplaceTempView("vw_df_flattened_pipeline_log_raw_success")

# COMMAND ----------

# DBTITLE 1,bronze table에 저장할 데이터의 컬럼 가공 및 필터링
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

bronze_success.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable("hansol_paper.managed.adf_pipeline_log_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ### pipelineruns Log - Failed 인 경우

# COMMAND ----------

# DBTITLE 1,작업 실패시 로그 필터링
df_pipeline_log_raw_fail = spark.sql("select * from vw_df_pipeline_log_raw where status in ('Failed')")

# COMMAND ----------

# DBTITLE 1,작업 실패시에만 로그 데이터 적재
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



# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view hansol_paper.managed.vw_adf_pipeline_log_monitor 
# MAGIC comment 'ADF(Azure Data Factory) Pipeline Log Monitor View'
# MAGIC as select pipelineName AS `파이프라인이름`
# MAGIC      , DATE_FORMAT(startTime_KST, 'MM/dd/yyyy, h:mm:ss a') AS `startTime`
# MAGIC      , DATE_FORMAT(endTime_KST, 'MM/dd/yyyy, h:mm:ss a')   AS `endTime`
# MAGIC      , lpad(FLOOR(cast(time_diff as int) / 3600),2,'0') || ':' || lpad(FLOOR((cast(time_diff as int) % 3600) / 60),2,'0')  || ':' || lpad(cast(time_diff as int) % 60,2,'0') as `기간`
# MAGIC      , `properties.Predecessors.Name` as `트리거기준`
# MAGIC      , status       AS `상태`
# MAGIC      , pipelineRunId
# MAGIC   from (
# MAGIC       select  unix_timestamp(t.endTime_KST) - unix_timestamp(t.startTime_KST) as time_diff
# MAGIC             , t.*
# MAGIC         from hansol_paper.managed.adf_pipeline_log_bronze  t
# MAGIC       order by t.endTime_KST desc
# MAGIC         ) 
# MAGIC   ;

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE hansol_paper.managed.adf_pipeline_log_bronze IS 'ADF(Azure Data Factory) Pipeline Log Bronze';
# MAGIC COMMENT ON TABLE hansol_paper.managed.adf_activity_log_bronze IS 'ADF(Azure Data Factory) Activity Log Bronze';

# COMMAND ----------


