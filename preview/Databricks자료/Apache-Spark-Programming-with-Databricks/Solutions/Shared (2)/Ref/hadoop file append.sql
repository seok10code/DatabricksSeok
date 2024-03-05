-- Databricks notebook source
CREATE TABLE tmon_hadoop.db.analytics_event_log (log_type string,
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
    LOCATION 'abfss://tmon@bigdatatmonadls001.dfs.core.windows.net/delta/db/analytics_event_log'
    PARTITIONED BY(dt);

-- COMMAND ----------

CREATE TABLE tmon_hadoop.db.analytics_page_log(log_date string,
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
    LOCATION 'abfss://tmon@bigdatatmonadls001.dfs.core.windows.net/delta/db/analytics_page_log'
    PARTITIONED BY(dt);

-- COMMAND ----------

CREATE TABLE tmon_hadoop.db.buy_log(
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
    LOCATION 'abfss://tmon@bigdatatmonadls001.dfs.core.windows.net/delta/db/buy_log'
    PARTITIONED BY(dt);
    

-- COMMAND ----------

insert into tmon_hadoop.db.analytics_event_log select * from hive_metastore.db.analytics_event_log;

-- COMMAND ----------

describe history tmon_hadoop.db.analytics_event_log

-- COMMAND ----------

insert into tmon_hadoop.db.analytics_page_log select * from hive_metastore.db.analytics_page_log;

-- COMMAND ----------

insert into tmon_hadoop.db.buy_log select * from hive_metastore.db.buy_log;
