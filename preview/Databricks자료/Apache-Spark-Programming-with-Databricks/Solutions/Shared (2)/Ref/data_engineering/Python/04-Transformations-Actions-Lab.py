# Databricks notebook source
# MAGIC
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations and Actions Lab
# MAGIC **Goal:** Read data with Apache Spark.
# MAGIC * Instructions are provided below, along with an empty cell for you to do your work.
# MAGIC * Click the + icon at the bottom of a cell to add additional empty cells.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Instructions
# MAGIC 0. Start with the file **"dbfs:/mnt/training/initech/products/dirty.csv"**
# MAGIC 0. Apply the necessary transformations and actions to return a DataFrame which satisfies these requirements:
# MAGIC
# MAGIC   a. Select just the `product_id`, `category`, `brand`, `model`, `size` and `price` columns
# MAGIC
# MAGIC   b. Rename `product_id` to `prodID`
# MAGIC
# MAGIC   c. Sort by the `price` column in descending order
# MAGIC
# MAGIC 0. Write the transformed DataFrame in parquet format to **"{userhome}/training/initech/Products.parquet"** You can get
# MAGIC    your home directory from the `userhome` variable.

# COMMAND ----------

# MAGIC %md
# MAGIC First, run the following cell to verify `/mnt/training` is properly configured.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC In the next cell, verify that `userhome` is set.

# COMMAND ----------


print(userhome)

# COMMAND ----------

# TODO
from pyspark.sql.functions import desc
inputFile = "dbfs:/mnt/training/initech/products/dirty.csv"

# Read from the CSV file
initialDF = (spark.read
  <<FILL_IN>>
)

# Your transformations
finalDF = <<FILL_IN>>

# Check that your transformations look good
finalDF.show()

# COMMAND ----------

# TODO
# Write to the Parquet
finalDF.write.<<FILL_IN>>

# COMMAND ----------

# MAGIC %md
# MAGIC First, let's see if some of the entries match

# COMMAND ----------

# TEST - Run this cell to test your solution.

assert finalDF.take(1)[0][1] == "Laptops "
assert finalDF.take(1)[0][2] == "Microsoft"

print("Congratulations, all tests passed!\n")

# COMMAND ----------

# MAGIC %md
# MAGIC Now, write to `{userhome}/training/initech/Products.parquet`

# COMMAND ----------


OUTPUT_FILE = userhome + "/training/initech/Products.parquet"

# COMMAND ----------

# TEST - Run this cell to test your solution.

def exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if "java.io.FileNotFoundException" in e.message:
      return False
    raise e

parquet = OUTPUT_FILE
assert exists(parquet), "{} does not exist".format(parquet)
assert finalDF.count() == spark.read.parquet(parquet).count()

print("Congratulations, all tests passed!\n")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
