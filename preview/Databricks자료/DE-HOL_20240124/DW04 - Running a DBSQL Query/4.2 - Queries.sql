-- Databricks notebook source
-- Q2. Simple aggregation: sales per product_category
USE 실습사용자명;
SELECT SUM(total_price) total_sales,
    product_category
FROM sales_gold
GROUP BY product_category ;

-- COMMAND ----------

-- Q3. sales per state
USE 실습사용자명;
SELECT c.state,
    COUNT(s.customer_id) AS cust_count,
    SUM(s.total_price) AS sales_revenue
FROM sales_gold s
JOIN customers c ON c.customer_id = s.customer_id
GROUP BY c.state ;

-- COMMAND ----------

-- Q4. total sales for Alert
USE 실습사용자명;
SELECT sum(total_price) AS total_sales,
    count(customer_id) AS cust_cnt
FROM sales_gold ;
