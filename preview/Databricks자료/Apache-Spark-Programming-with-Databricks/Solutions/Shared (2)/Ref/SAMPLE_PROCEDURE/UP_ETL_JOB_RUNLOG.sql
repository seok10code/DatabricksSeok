-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("vts_start_dttm", "2022-05-08 00:00:00")
-- MAGIC dbutils.widgets.text("vnv_job_name", "null")
-- MAGIC dbutils.widgets.text("vnv_procedure_name", "null")
-- MAGIC dbutils.widgets.text("vsi_process_step", "null")
-- MAGIC dbutils.widgets.text("vnv_job_status", "null")
-- MAGIC dbutils.widgets.text("vts_finish_dttm", "2022-05-09 00:00:00")
-- MAGIC dbutils.widgets.text("vnv_elapsed_time", "null")
-- MAGIC dbutils.widgets.text("vnv_source_table_name", "null")
-- MAGIC dbutils.widgets.text("vii_source_cnt", "null")
-- MAGIC dbutils.widgets.text("vnv_target_table_name", "null")
-- MAGIC dbutils.widgets.text("vii_target_cnt", "null")
-- MAGIC dbutils.widgets.text("vnv_error_msg", "null")
-- MAGIC dbutils.widgets.text("vnv_job_arguments", "null")
-- MAGIC dbutils.widgets.text("vnv_job_description", "null")

-- COMMAND ----------

select '${vts_start_dttm}'
                          ,'${vnv_job_name}'
                          ,'${vnv_procedure_name}'
                          ,'${vsi_process_step}'
                          ,'${vnv_job_status}'
                          ,'${vts_finish_dttm}'
                          ,'${vnv_elapsed_time}'
                          ,'${vnv_source_table_name}'
                          ,'${vii_source_cnt}'
                          ,'${vnv_target_table_name}'
                          ,'${vii_target_cnt}'
                          ,'${vnv_error_msg}'
                          ,'${vnv_job_arguments}'
                          ,'${vnv_job_description}'

-- COMMAND ----------

INSERT into tmon_netezza.log.job_runlog values('${vts_start_dttm}'
                          ,'${vnv_job_name}'
                          ,'${vnv_procedure_name}'
                          ,'${vsi_process_step}'
                          ,'${vnv_job_status}'
                          ,'${vts_finish_dttm}'
                          ,'${vnv_elapsed_time}'
                          ,'${vnv_source_table_name}'
                          ,'${vii_source_cnt}'
                          ,'${vnv_target_table_name}'
                          ,'${vii_target_cnt}'
                          ,'${vnv_error_msg}'
                          ,'${vnv_job_arguments}'
                          ,'${vnv_job_description}'
                          ,now());
