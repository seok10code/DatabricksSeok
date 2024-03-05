# Databricks notebook source
# MAGIC %md
# MAGIC # ADF(Azure DataFactory) 모니터링 및 오브젝트 통계정보 조회 가이드

# COMMAND ----------

# DBTITLE 1,Databricks 관리용 오브젝트 목록 조회
# MAGIC %sql
# MAGIC select t.comment, t.*
# MAGIC   from system.information_schema.tables t
# MAGIC  where table_catalog = 'hansol_paper' and table_schema = 'managed'  and (table_name like 'adf%' or table_name like 'vw%' or table_name like 'table%')
# MAGIC order by table_name 
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,ADF pipeline Log Monitor View 조회
# MAGIC %sql
# MAGIC select * from hansol_paper.managed.vw_adf_pipeline_log_monitor limit 10 ;

# COMMAND ----------

# DBTITLE 1,ADF activity Log Monitor View 조회
# MAGIC %sql
# MAGIC select * from hansol_paper.managed.vw_adf_activity_log_monitor t limit 10 ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hansol_paper.managed.table_stats_info order by std_time desc;

# COMMAND ----------

# DBTITLE 1,Each Pipeline Log Status 조회
# MAGIC %sql
# MAGIC select t.* from hansol_paper.managed.adf_pipeline_log_bronze t where t.pipelineRunId = 'ade85dc8-4c7a-40d6-a0a1-f569d8af9bde';

# COMMAND ----------

# DBTITLE 1,Each Activity Log Status 조회
# MAGIC %sql
# MAGIC select t.*
# MAGIC from hansol_paper.managed.adf_activity_log_bronze t 
# MAGIC where t.activityRunId = 'd688efc0-029b-49b2-a761-19ce7c2e9a16' ;
