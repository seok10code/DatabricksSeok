# Databricks notebook source
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Connection String
ev_namespace    ="bigdata-eh-tmon-001"
ev_name         ="eventlogtest"
ev_sas_key_name ="accesstest"
ev_sas_key_val  = "luFjgrBWsJD3DiBqwtKSQexLDUgDY4zbN+AEhBFc/KY="


conn_string="Endpoint=sb://{0}.servicebus.windows.net/;EntityPath={1};SharedAccessKeyName={2};SharedAccessKey={3}".format(ev_namespace, ev_name, ev_sas_key_name, ev_sas_key_val)

ehConf = {}
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn_string)

# COMMAND ----------

df = spark.readStream.format("eventhubs").options(**ehConf).load()
display(df)
