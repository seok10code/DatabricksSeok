-- Databricks notebook source
-- MAGIC %python
-- MAGIC databricks_user = spark.sql("SELECT current_user()").collect()[0][0].split('@')[0].replace(".", "_")
-- MAGIC
-- MAGIC spark.sql("DROP DATABASE IF EXISTS delta_{}_db CASCADE".format(str(databricks_user)))
-- MAGIC spark.sql("CREATE DATABASE IF NOT EXISTS delta_{}_db".format(str(databricks_user)))
-- MAGIC spark.sql("USE delta_{}_db".format(str(databricks_user)))
-- MAGIC print("데이터베이스명 : delta_{}_db".format((databricks_user)))

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_data_raw_csv(
  userid STRING,
  gender STRING,
  age INT,
  height INT,
  weight INT,
  smoker STRING,
  familyhistory STRING,
  chosestlevs STRING,
  bp STRING,
  risk INT)
USING csv
OPTIONS (path "/databricks-datasets/iot-stream/data-user/userData.csv", header "true");

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS device_data_raw_json(
--컬럼 내용 추가
  calories_burnt DOUBLE,
  device_id INT,
  id STRING,
  miles_walked DOUBLE,
  num_steps INT,
  `timestamp` TIMESTAMP,
  user_id STRING,
  value STRING
)
USING JSON
OPTIONS (path "/databricks-datasets/iot-stream/data-device")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS device_data_bronze_delta
USING DELTA
PARTITIONED BY (device_id)
AS SELECT * FROM JSON.`/databricks-datasets/iot-stream/data-device`;

SELECT * FROM device_data_bronze_delta;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_data_bronze_delta 
USING DELTA
PARTITIONED BY (gender)
COMMENT "User Data Raw Table - No Transformations"
AS SELECT * FROM user_data_raw_csv;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_data_silver
USING DELTA
  SELECT 
    userid AS user_id,
    gender,
    age,
    height,
    weight,
    smoker,
    familyhistory AS family_history,
    chosestlevs AS cholest_levs,
    bp AS blood_pressure,
    risk
  FROM user_data_bronze_delta;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS device_data_silver
USING DELTA
  SELECT
    id,
    device_id,
    user_id,
    calories_burnt,
    miles_walked,
    num_steps,
    miles_walked/num_steps as stride,
    timestamp,
    DATE(timestamp) as date
  FROM device_data_bronze_delta;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_daily_averages_gold
USING DELTA
  SELECT
      u.user_id,
      u.gender,
      u.age,
      u.blood_pressure,
      u.cholest_levs,
      AVG(calories_burnt) as avg_calories_burnt,
      AVG(num_steps) as avg_num_steps,
      AVG(miles_walked) as avg_miles_walked
    FROM user_data_silver u
    LEFT JOIN
      (SELECT 
        user_id,
        date,
        MAX(calories_burnt) as calories_burnt,
        MAX(num_steps) as num_steps,
        MAX(miles_walked) as miles_walked
      FROM device_data_silver
      GROUP BY user_id, date) as daily
    ON daily.user_id = u.user_id
    GROUP BY u.user_id,
      u.gender,
      u.age,
      u.blood_pressure,
      u.cholest_levs
