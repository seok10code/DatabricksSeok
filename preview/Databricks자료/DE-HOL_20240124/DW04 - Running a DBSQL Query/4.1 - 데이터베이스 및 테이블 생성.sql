-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 1,Step 1. 데이터베이스 생성
-- MAGIC %python
-- MAGIC databricks_user = spark.sql("SELECT current_user()").collect()[0][0].split('@')[0].replace(".", "_")
-- MAGIC print(databricks_user)
-- MAGIC spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(databricks_user))
-- MAGIC spark.sql("USE {}".format(databricks_user))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("databaseName",databricks_user)

-- COMMAND ----------

-- DBTITLE 1,Step 2. 실습용 테이블 생성
CREATE TABLE IF NOT EXISTS customers USING csv OPTIONS (
  path "/databricks-datasets/retail-org/customers",
  header "true",
  inferSchema "true"
);

CREATE TABLE IF NOT EXISTS sales_gold 
USING delta LOCATION "/databricks-datasets/retail-org/solutions/gold/sales";

CREATE TABLE IF NOT EXISTS silver_purchase_orders 
USING delta LOCATION "/databricks-datasets/retail-org/solutions/silver/purchase_orders.delta";

CREATE TABLE IF NOT EXISTS silver_sales_orders 
USING delta LOCATION "/databricks-datasets/retail-org/solutions/silver/sales_orders";

CREATE TABLE IF NOT EXISTS source_silver_suppliers 
USING delta LOCATION "/databricks-datasets/retail-org/solutions/silver/suppliers";


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
