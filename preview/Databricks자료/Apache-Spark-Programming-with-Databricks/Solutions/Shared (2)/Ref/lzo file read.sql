-- Databricks notebook source
CREATE EXTERNAL TABLE hive_metastore.db.analytics_event_log (log_type string,
log_date string,
platform string,
os_version string,
app_version string,
agent string,
ip string,
msrl bigint,
pid string,
sid string,
ad_id string,
start string,
campaign map<string,string>,
ab_name map<string,string>,
info_session map<string,string>,
host string,
page string,
page_dims map<string,string>,
referrer_host string,
referrer_page string,
referrer_dims map<string,string>,
event string,
event_type string,
area string,
ord bigint,
event_dims map<string,string>,
dt string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
--    STORED AS INPUTFORMAT 'com.ly.spark.example.serde.io.SerDeExampleInputFormat'
--        OUTPUTFORMAT 'com.ly.spark.example.serde.io.SerDeExampleOutputFormat'
    LOCATION '/mnt/test/db/analytics_event_log'
    PARTITIONED BY(dt);

-- COMMAND ----------

CREATE EXTERNAL TABLE hive_metastore.db.analytics_page_log(log_date string,
platform string,
os_version string,
app_version string,
agent string,
ip string,
msrl bigint,
pid string,
sid string,
ad_id string,
start string,
campaign map<string,string>,
ab_name map<string,string>,
info_session map<string,string>,
host string,
page string,
page_dims map<string,string>,
referrer_host string,
referrer_page string,
referrer_dims map<string,string>,
dt string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
--    STORED AS INPUTFORMAT 'com.ly.spark.example.serde.io.SerDeExampleInputFormat'
--        OUTPUTFORMAT 'com.ly.spark.example.serde.io.SerDeExampleOutputFormat'
    LOCATION '/mnt/test/db/analytics_page_log'
    PARTITIONED BY(dt);

-- COMMAND ----------

CREATE EXTERNAL TABLE hive_metastore.db.buy_log(
created_at string,
main_deal_srl bigint,
opt_deal_srl bigint,
main_buy_srl string,
ticket_cnt int,
amount bigint,
member_srl bigint,
pid string,
sid string,
adid string,
ab_name map<string,string>,
user_ip string,
platform string,
app_version string,
q map<string,string>,
dt string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
--    STORED AS INPUTFORMAT 'com.ly.spark.example.serde.io.SerDeExampleInputFormat'
--        OUTPUTFORMAT 'com.ly.spark.example.serde.io.SerDeExampleOutputFormat'
    LOCATION '/mnt/test/db/buy_log'
    PARTITIONED BY(dt);
    

-- COMMAND ----------

msck repair table hive_metastore.db.analytics_event_log;
msck repair table hive_metastore.db.analytics_page_log;
msck repair table hive_metastore.db.buy_log;

-- COMMAND ----------

select * from hive_metastore.db.analytics_event_log;

-- COMMAND ----------

drop table hive_metastore.db.analytics_event_log;

-- COMMAND ----------

select DISTINCT dt from hive_metastore.db.analytics_page_log;

-- COMMAND ----------

select * from hive_metastore.db.buy_log;
