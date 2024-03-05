# Databricks notebook source
# MAGIC %md
# MAGIC # 테이블 통계 정보 생성

# COMMAND ----------

# DBTITLE 1,describe detail table info 및 rows 조회 뷰 생성(vw_df_all_tbl_details / vw_df_table_rows)
# Table Names
tables_list = [
    tables.table_name
    for tables in spark.sql("""SELECT table_catalog || '.' || table_schema || '.' || table_name as table_name
from system.information_schema.tables t
 where 0=0
   and table_catalog not in ('system','samples','main','hive_metastore')
   and data_source_format = 'DELTA'
""").collect()
]

def chk_null(a):
    return '' if a is None else a

# Table details
table_details = [{
'id':row['id'],
'name':row['name'],
'description': chk_null(row['description']),
'location':row['location'],
'createdAt':row['createdAt'],
'lastModified':row['lastModified'],
'partitionColumns': " ".join(row['partitionColumns']),
'numFiles':row['numFiles'],
'sizeInBytes':row['sizeInBytes'],
'minReaderVersion':row['minReaderVersion'],
'minWriterVersion':row['minWriterVersion']
}
for tbl_rows in [
  spark.sql(f'describe detail {tbl}').collect() for tbl in tables_list
]
for row in tbl_rows
]
#Print extended table details
df_all_tbl_details = spark.createDataFrame(table_details)
df_all_tbl_details.createOrReplaceTempView("vw_df_all_tbl_details")

table_rows = [{
'tab_nm':row['tab_nm'],
'tab_cnt':row['tab_cnt']
}
for tbl_rows in [
  spark.sql(f"select '{tbl}' as tab_nm, count(*) as tab_cnt from {tbl}").collect() for tbl in tables_list
]
for row in tbl_rows
]

df_table_rows = spark.createDataFrame(table_rows)
df_table_rows.createOrReplaceTempView("vw_df_table_rows")

# COMMAND ----------

# DBTITLE 1,table_stats_info 테이블 생성
# MAGIC %sql
# MAGIC --drop table if exists hansol_paper.managed.table_stats_info ;
# MAGIC --create table hansol_paper.managed.table_stats_info as
# MAGIC insert into hansol_paper.managed.table_stats_info 
# MAGIC select 
# MAGIC        date_format(now(), "yyyy-MM-dd HH:mm:ss") as std_time
# MAGIC      , SPLIT(t.name, '\\.')[0] as catalog_name
# MAGIC      , SPLIT(t.name, '\\.')[1] as schema_name
# MAGIC      , SPLIT(t.name, '\\.')[2] as table_name
# MAGIC      , t.numFiles
# MAGIC      , round(t.sizeinbytes/power(1024,2),2) as size_MB
# MAGIC      , t2.tab_cnt 
# MAGIC      , t.createdAt
# MAGIC      , t.lastModified
# MAGIC      , t.location
# MAGIC      , t.minReaderVersion
# MAGIC      , t.minWriterVersion
# MAGIC   from vw_df_all_tbl_details t left outer join vw_df_table_rows t2 on t.name = t2.tab_nm 
# MAGIC order by 2 ;
