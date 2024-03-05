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

-- MAGIC %md <i18n value="09bc5c4d-1408-47f8-8102-ddaf0a96a6c0"/>
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## 실버 레이어 테이블 선언하기
-- MAGIC
-- MAGIC 이제 실버 레이어를 구현하는 테이블을 선언합니다. 이 레이어는 다운스트림 애플리케이션을 최적화하기 위해 브론즈 레이어의 정제된 데이터 복사본을 나타냅니다. 이 수준에서는 데이터 정리 및 보강과 같은 작업을 적용합니다.

-- COMMAND ----------

-- MAGIC %md <i18n value="eb20ea4d-1115-4536-adac-7bbd85491726"/>
-- MAGIC
-- MAGIC
-- MAGIC ### sales_orders_cleaned
-- MAGIC
-- MAGIC 여기서 우리는 null 주문 번호가 있는 레코드를 거부하여 품질 관리를 구현하는 것 외에도 고객 정보로 판매 거래 데이터를 풍부하게 하는 첫 번째 실버 테이블을 선언합니다.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,주문 번호가 NULL 인 데이터 집계
SELECT COUNT (*)
FROM sales_orders_raw
WHERE order_number is null;

-- COMMAND ----------

-- DBTITLE 1,sales_orders_cleaned 테이블 생성
CREATE OR REPLACE TABLE sales_orders_cleaned
COMMENT "유효한 주문 번호가 있는 클린징된 판매 주문 데이터" AS
SELECT
  f.customer_id,
  f.customer_name,
  f.number_of_line_items,
  timestamp(from_unixtime((cast(f.order_datetime as long)))) as order_datetime,
  date(from_unixtime((cast(f.order_datetime as long)))) as order_date,
  f.order_number,
  f.ordered_products,
  c.state,
  c.city,
  c.lon,
  c.lat,
  c.units_purchased,
  c.loyalty_segment
FROM
  sales_orders_raw f
  LEFT JOIN customers c ON c.customer_id = f.customer_id
  AND c.customer_name = f.customer_name
  AND f.order_number is not null

-- COMMAND ----------

SELECT * FROM sales_orders_cleaned LIMIT 100;

-- COMMAND ----------


