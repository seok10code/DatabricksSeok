-- Databricks notebook source
-- MAGIC %python
-- MAGIC #try:
-- MAGIC #    dbutils.notebook.run("/Shared/SAMPLE_PROCEDURE/Task1", 0,{"vnc_in_std_ymd": "2023-05-03T05:31:11.702+0000", "vnc_in_end_ymd":"2023-05-04T05:31:11.702+0000","vnv_in_job_name":"cloocus_test"})
-- MAGIC #except:
-- MAGIC #    print('a')

-- COMMAND ----------

-- MAGIC %run /Shared/SAMPLE_PROCEDURE/UP_ETL_JOB_RUNLOG $vts_start_dttm="2023-05-01T05:31:11.702+0000" $vts_finish_dttm="2023-05-04T05:31:11.702+0000"

-- COMMAND ----------

-- MAGIC %run /Shared/SAMPLE_PROCEDURE/Task1 $vnc_in_std_ymd="2023-05-03T05:31:11.702+0000" $vnc_in_end_ymd="2023-05-04T05:31:11.702+0000" $vnv_in_job_name="cloocus_test"

-- COMMAND ----------

select ${var.vsi_process_step}
