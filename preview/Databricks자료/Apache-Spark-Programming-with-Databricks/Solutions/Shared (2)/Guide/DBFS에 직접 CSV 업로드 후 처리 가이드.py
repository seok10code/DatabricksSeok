# Databricks notebook source
# DBTITLE 1,워크스페이스에 파일 업로드시 해당 위치 확인(ls 명령어)
!ls -al /Workspace/Shared/Upload/

# COMMAND ----------

# DBTITLE 1,해당 파일은 DBFS에 위치하지 않기 때문에 처리 불가
datapath = "/Workspace/Shared/Upload/card_transaction.v1_10000.csv"
df = spark.read.format("csv")\
              .option("header","true")\
              .option("inferschema","true")\
              .load(datapath)
df.display()  

# COMMAND ----------

# DBTITLE 1,DBFS 파일 업로드(신규>파일 upload > DBFS에 upload 클릭)시 해당 위치 확인(fs ls 명령어 사용)
# MAGIC %fs ls /FileStore/tables

# COMMAND ----------

# DBTITLE 1,해당 파일은 DBFS에 존재하기 때문에 처리 가능
datapath = "dbfs:/user/hive/warehouse/card_transaction.v1_10000.csv"
df = spark.read.format("csv")\
              .option("header","true")\
              .option("inferschema","true")\
              .load(datapath)
df.limit(5).display()

# COMMAND ----------


