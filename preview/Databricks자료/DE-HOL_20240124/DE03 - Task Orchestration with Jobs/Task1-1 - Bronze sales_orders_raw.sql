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

-- MAGIC %md <i18n value="458afeb2-4472-4ed2-a4c7-6dea2338c3f2"/>
-- MAGIC
-- MAGIC
-- MAGIC ### sales_orders_raw
-- MAGIC
-- MAGIC **`sales_orders_raw`** 는 **retail-org/sales_orders** 데이터 세트에서 점진적으로 JSON 데이터를 수집합니다.
-- MAGIC
-- MAGIC 아래 선언은 또한 데이터 카탈로그를 탐색하는 모든 사용자에게 표시되는 추가 테이블 메타데이터(이 경우 주석 및 속성)의 선언을 보여줍니다.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sales_orders_raw
USING JSON
COMMENT "retail-org/sales_orders에서 수집된 원시 판매 주문 데이터"
OPTIONS (path "${datasets_path}/retail-org/sales_orders");
