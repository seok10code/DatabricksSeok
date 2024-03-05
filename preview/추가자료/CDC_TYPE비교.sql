-- Databricks notebook source

select * from hive_metastore.default.customer_silver limit 10

-- COMMAND ----------


select * from hive_metastore.default.customer_silver2 limit 100

-- COMMAND ----------

select count(*) from hive_metastore.default.customer_silver;

-- COMMAND ----------

select count(*) from hive_metastore.default.customer_silver2;

-- COMMAND ----------

select count(*) from hive_metastore.default.customer_bronze2;

-- COMMAND ----------

select * from hive_metastore.default.customer_silver2 where id = '003f7a23-7901-4892-acc1-422bd8503e97'

-- COMMAND ----------

select * from hive_metastore.default.customer_bronze2 where id = '003f7a23-7901-4892-acc1-422bd8503e97'

-- COMMAND ----------

select count(*) from hive_metastore.default.customer_silver2 where __START_AT is not null

-- COMMAND ----------

-- drop table hive_metastore.default.customer_bronze2;
drop View hive_metastore.default.customer_silver2;

-- COMMAND ----------

select count(*) from hive_metastore.default.customer_silver2;

-- COMMAND ----------

select count(*) from hive_metastore.default.customer_silver;

-- COMMAND ----------

select * from hive_metastore.default.customer_silver2 limit 10

-- COMMAND ----------

desc formatted hive_metastore.default.customer_bronze2

-- COMMAND ----------

desc formatted hive_metastore.default.customer_silver2

-- COMMAND ----------

desc formatted hive_metastore.default.customer_bronze2

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC ls dbfs:/pipelines/4ca7244b-0de7-4efc-afe0-1697b220a861/tables/customer_bronze2

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC ls dbfs:/pipelines/4ca7244b-0de7-4efc-afe0-1697b220a861/tables/customer_bronze2/_delta_log/

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC head dbfs:/pipelines/4ca7244b-0de7-4efc-afe0-1697b220a861/tables/customer_bronze2/_delta_log/00000000000000000000.crc

-- COMMAND ----------

select * from hive_metastore.default.customer_bronze2

-- COMMAND ----------

insert into hive_metastore.default.customer_bronze2 select * from hive_metastore.default.customer_bronze2 limit 10

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC ls dbfs:/pipelines/4ca7244b-0de7-4efc-afe0-1697b220a861/tables/customer_bronze2/_delta_log/

-- COMMAND ----------

desc history hive_metastore.default.customer_bronze2  

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC head dbfs:/pipelines/4ca7244b-0de7-4efc-afe0-1697b220a861/tables/customer_bronze2/_delta_log/00000000000000000002.json

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC head dbfs:/pipelines/4ca7244b-0de7-4efc-afe0-1697b220a861/tables/customer_bronze2/_delta_log/00000000000000000002.crc

-- COMMAND ----------

optimize hive_metastore.default.customer_bronze2  

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC ls dbfs:/pipelines/4ca7244b-0de7-4efc-afe0-1697b220a861/tables/customer_bronze2/_delta_log/
-- MAGIC 00000000000000000003.json

-- COMMAND ----------

select * from hive_metastore.default.customer_bronze2  limit 100

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC head dbfs:/pipelines/4ca7244b-0de7-4efc-afe0-1697b220a861/tables/customer_bronze2/_delta_log/00000000000000000003.json

-- COMMAND ----------

optimize hive_metastore.default.customer_bronze2  zorder by(lastname)

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC ls dbfs:/pipelines/4ca7244b-0de7-4efc-afe0-1697b220a861/tables/customer_bronze2/_delta_log/

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC
-- MAGIC ls -lt /dbfs/pipelines/4ca7244b-0de7-4efc-afe0-1697b220a861/tables/customer_bronze2/

-- COMMAND ----------

select * from hive_metastore.default.customer_bronze2

-- COMMAND ----------

optimize hive_metastore.default.customer_bronze2  zorder by(lastname, firstname)

-- COMMAND ----------

explain formatted select * from hive_metastore.default.customer_bronze2 where lastname = 'Young'

-- COMMAND ----------

select * from hive_metastore.default.customer_bronze2 where lastname = 'Young'

-- COMMAND ----------

select * from hive_metastore.default.customer_bronze2 where lastname = 'Young' AND firstname = 'John'

-- COMMAND ----------


