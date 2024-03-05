# Databricks notebook source
# MAGIC
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Data Lab
# MAGIC **Goal:** Read data with Apache Spark.
# MAGIC * Instructions are provided below along with empty cells for you to do your work.
# MAGIC * Additional cells are provided at the bottom of this notebook to verify your work is accurate.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Instructions
# MAGIC 0. Start with the file **dbfs:/mnt/training/initech/products/dirty.csv**, a file containing product details.
# MAGIC 0. Read the data and assign it to a `DataFrame` named **newProductDF**.
# MAGIC 0. Run the last cell to verify the data was loaded correctly and to print its schema.
# MAGIC
# MAGIC Bonus: Create the `DataFrame` and print its schema **without** executing a single job.

# COMMAND ----------

# MAGIC %md
# MAGIC First, run the following cell to verify `/mnt/training` is properly configured.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Show Your Work

# COMMAND ----------

# TODO

csvFile = "dbfs:/mnt/training/initech/products/dirty.csv"

newProductDF = (spark.read               # The DataFrameReader
  <<FILL_IN>>
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Verify Your Work
# MAGIC Run the following cell to verify the `DataFrame` was created properly.

# COMMAND ----------

# TEST - Run this cell to test your solution.
newProductDF.printSchema()

columns = newProductDF.dtypes
assert len(columns) == 8, "Expected 8 columns but found " + str(len(columns))

assert columns[0][0] == "product_id", "Expected column 0 to be \"product_id\" but found \"" + columns[0][0] + "\"."
assert columns[0][1] == "int",        "Expected column 0 to be of type \"int\" but found \"" + columns[0][1] + "\"."

assert columns[1][0] == "category",   "Expected column 1 to be \"category\" but found \"" + columns[1][0] + "\"."
assert columns[1][1] == "string",     "Expected column 1 to be of type \"string\" but found \"" + columns[1][1] + "\"."

assert columns[2][0] == "brand",      "Expected column 2 to be \"brand\" but found \"" + columns[2][0] + "\"."
assert columns[2][1] == "string",     "Expected column 2 to be of type \"string\" but found \"" + columns[2][1] + "\"."

assert columns[3][0] == "model",      "Expected column 3 to be \"model\" but found \"" + columns[3][0] + "\"."
assert columns[3][1] == "string",     "Expected column 3 to be of type \"string\" but found \"" + columns[3][1] + "\"."

assert columns[4][0] == "price",      "Expected column 4 to be \"price\" but found \"" + columns[4][0] + "\"."
assert columns[4][1] == "double",     "Expected column 4 to be of type \"double\" but found \"" + columns[4][1] + "\"."

assert columns[5][0] == "processor",  "Expected column 5 to be \"processor\" but found \"" + columns[5][0] + "\"."
assert columns[5][1] == "string",     "Expected column 5 to be of type \"string\" but found \"" + columns[5][1] + "\"."

assert columns[6][0] == "size",       "Expected column 6 to be \"size\" but found \"" + columns[6][0] + "\"."
assert columns[6][1] == "string",     "Expected column 6 to be of type \"string\" but found \"" + columns[6][1] + "\"."

assert columns[7][0] == "display",    "Expected column 7 to be \"display\" but found \"" + columns[7][0] + "\"."
assert columns[7][1] == "string",     "Expected column 7 to be of type \"string\" but found \"" + columns[7][1] + "\"."

print("Congratulations, all tests passed!\n")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
