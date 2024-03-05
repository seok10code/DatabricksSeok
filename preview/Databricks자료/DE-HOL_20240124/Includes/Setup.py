# Databricks notebook source
# user 별 database및 변수 설정 
databricks_user = spark.sql("SELECT current_user()").collect()[0][0].split('@')[0].replace(".", "_")
print(databricks_user)

# spark.sql("DROP DATABASE IF EXISTS {} CASCADE".format(str(databricks_user)))
spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(str(databricks_user)))
spark.sql("USE {}".format(str(databricks_user)))
