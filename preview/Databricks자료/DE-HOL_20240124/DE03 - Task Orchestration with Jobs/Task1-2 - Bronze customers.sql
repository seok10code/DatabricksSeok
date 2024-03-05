-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC databricks_user = spark.sql("SELECT current_user()").collect()[0][0].split('@')[0].replace(".", "_")
-- MAGIC spark.sql(f"USE SCHEMA {databricks_user}")

-- COMMAND ----------

-- MAGIC %md <i18n value="1cc1fe77-a235-40dd-beb5-bacc94425b96"/>
-- MAGIC
-- MAGIC
-- MAGIC ## 브론즈 레이어 테이블 선언하기
-- MAGIC
-- MAGIC 아래에서 브론즈 레이어를 구현하는 두 개의 테이블을 선언합니다. 이것은 가장 원시적인 형태의 데이터를 나타내지만, 무기한 보존할 수 있는 형식으로 캡처되고 Delta Lake가 제공해야 하는 성능과 이점을 가지고 쿼리됩니다.

-- COMMAND ----------

-- MAGIC %md <i18n value="57accbe1-e96d-44ca-8937-8849634d1da2"/>
-- MAGIC
-- MAGIC
-- MAGIC ### customers
-- MAGIC **`customers`** 는 **retail-org/customers** 에 있는 CSV 고객 데이터를 제공합니다.
-- MAGIC
-- MAGIC 이 테이블은 조인 작업에서 곧 사용되어 판매 기록을 기반으로 고객 데이터를 조회합니다.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS customers
USING CSV
COMMENT "retail-org/customers 에서 수집된 완제품을 구매하는 고객 데이터"
OPTIONS (path "${datasets_path}/retail-org/customers/",header "true",inferSchema "true");
