-- Databricks notebook source
msck repair table hive_metastore.db.analytics_event_log;
msck repair table hive_metastore.db.analytics_page_log;
msck repair table hive_metastore.db.buy_log;

-- COMMAND ----------

insert into tmon_hadoop.db.analytics_event_log select * from hive_metastore.db.analytics_event_log where dt = date_add(to_date(timestampadd(HOUR, 9, current_timestamp())), -1);
insert into tmon_hadoop.db.analytics_page_log select * from hive_metastore.db.analytics_page_log where dt = date_add(to_date(timestampadd(HOUR, 9, current_timestamp())), -1);
insert into tmon_hadoop.db.buy_log select * from hive_metastore.db.buy_log where dt = date_add(to_date(timestampadd(HOUR, 9, current_timestamp())), -1);

-- COMMAND ----------


