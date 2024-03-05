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

-- MAGIC %md <i18n value="ae062501-5a39-4183-976a-53662619d516"/>
-- MAGIC
-- MAGIC
-- MAGIC ## 골드 테이블 선언 (상위 판매 상품 집계)
-- MAGIC
-- MAGIC 아키텍처의 가장 정제된 수준에서 우리는 비즈니스 가치가 있는 집계를 제공하는 테이블을 선언합니다.  
-- MAGIC 이 경우에는 특정 지역을 기반으로 하는 판매 주문 데이터 모음입니다. 집계 시 보고서는 날짜 및 고객별로 주문 수와 합계를 생성합니다

-- COMMAND ----------

CREATE
OR REPLACE TABLE product_by_sales COMMENT "제품 별 판매량" AS
SELECT
  ordered_products_explode.curr,
  ordered_products_explode.name,
  sum(ordered_products_explode.price * ordered_products_explode.qty) as sales,
  sum(ordered_products_explode.qty) as total_qty
FROM
  (
    SELECT
      city,
      order_date,
      customer_id,
      customer_name,
      explode(ordered_products) as ordered_products_explode
    FROM
      sales_orders_cleaned
  )
GROUP BY
  ordered_products_explode.name,
  ordered_products_explode.curr


-- COMMAND ----------

SELECT * FROM product_by_sales
ORDER BY sales DESC 
LIMIT 10;
