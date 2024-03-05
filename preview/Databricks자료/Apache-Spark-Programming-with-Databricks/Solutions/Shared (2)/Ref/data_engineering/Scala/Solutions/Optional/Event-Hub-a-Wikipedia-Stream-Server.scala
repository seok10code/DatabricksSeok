// Databricks notebook source
// MAGIC
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Wikipedia Edits Stream Server
// MAGIC
// MAGIC This notebook grabs data from a Wikipedia web server into Event Hubs.
// MAGIC
// MAGIC ### Library Requirements
// MAGIC
// MAGIC 1. the Maven library with coordinate `com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.7`
// MAGIC    - this allows Databricks `spark` session to communicate with an Event Hub
// MAGIC 2. the Python library `azure-eventhub`
// MAGIC    - this is allows the Python kernel to stream content to an Event Hub
// MAGIC 3. the Python library `sseclient`
// MAGIC    - this is used to create a streaming client to an existing streaming server
// MAGIC
// MAGIC Documentation on how to install Python libraries:
// MAGIC https://docs.azuredatabricks.net/user-guide/libraries.html#pypi-libraries
// MAGIC
// MAGIC Documentation on how to install Maven libraries:
// MAGIC https://docs.azuredatabricks.net/user-guide/libraries.html#maven-or-spark-package
// MAGIC
// MAGIC You can use <b>Run All</b> in the top menu bar next to <b>Permissions</b> to run this notebook.

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC dbutils.widgets.text("CONNECTION_STRING", "", "Connection String")
// MAGIC dbutils.widgets.text("EVENT_HUB_NAME", "wiki-changes", "Event Hub")
// MAGIC dbutils.widgets.text("EVENT_COUNT","1000","Event Count")

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC pcString = dbutils.widgets.get("CONNECTION_STRING")
// MAGIC uniqueEHName = dbutils.widgets.get("EVENT_HUB_NAME")
// MAGIC
// MAGIC # set this to number of records you want
// MAGIC eventCount = int(dbutils.widgets.get("EVENT_COUNT"))
// MAGIC
// MAGIC assert pcString != "", ": The Primary Connection String must be non-empty"
// MAGIC assert uniqueEHName != "", ": The Unique Event Hubs Name must be non-empty"
// MAGIC assert (eventCount != "") & (eventCount > 0), ": The number of events must be non-empty and greater than 0"
// MAGIC
// MAGIC connectionString = pcString.replace(".net/;", ".net/{}/;".format(uniqueEHName))

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Create an `EventHubClient` with a sender and run the client.
// MAGIC
// MAGIC Import Libraries necessary to run the server

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC from azure.eventhub import EventHubClient, Sender, EventData, Offset
// MAGIC from sseclient import SSEClient as EventSource
// MAGIC
// MAGIC eh = EventHubClient.from_connection_string(connectionString)
// MAGIC
// MAGIC sender = eh.add_sender(partition="0")
// MAGIC
// MAGIC eh.run()

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Stream events from the Wiki Streaming Server. This code breaks the loop after it hits `Event Count`.
// MAGIC
// MAGIC To read more events, alter the variable `Event Count` in the widget at the top.
// MAGIC
// MAGIC This cell can be run multiple times.

// COMMAND ----------

// MAGIC %python
// MAGIC wikiChangesURL = 'https://stream.wikimedia.org/v2/stream/recentchange'
// MAGIC
// MAGIC for i, event in enumerate(EventSource(wikiChangesURL)):
// MAGIC     if event.event == 'message' and event.data != '':
// MAGIC         sender.send(EventData(add_geo_data(event)))
// MAGIC     if i > eventCount:
// MAGIC         print("OK")
// MAGIC         break

// COMMAND ----------

// MAGIC %python
// MAGIC displayHTML("All done!")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
