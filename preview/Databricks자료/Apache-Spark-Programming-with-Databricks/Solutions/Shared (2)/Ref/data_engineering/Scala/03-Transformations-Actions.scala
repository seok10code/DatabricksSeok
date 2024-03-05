// Databricks notebook source
// MAGIC
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC #Introduction to Transformations and Actions
// MAGIC
// MAGIC **In this lesson:**
// MAGIC * Invoke the DataFrame APIs.
// MAGIC * Perform DataFrame transformations.
// MAGIC * Share and export notebooks.

// COMMAND ----------

// MAGIC %md
// MAGIC First, run the following cell to verify `/mnt/training` is properly configured.

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

// MAGIC %md
// MAGIC The data is located at `dbfs:/mnt/training/initech/products/dirty.csv`.

// COMMAND ----------

// MAGIC %fs ls /mnt/training/initech/

// COMMAND ----------


val CSV_FILE = "dbfs:/mnt/training/initech/products/dirty.csv"

val retailDF = spark.read
  .option("header", true)
  .option("inferSchema", true)
  .csv(CSV_FILE)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Our Data
// MAGIC
// MAGIC Take a look at the type of data available, the schema Spark inferred from our file.
// MAGIC
// MAGIC Do this with the `printSchema()` method:

// COMMAND ----------


retailDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC You should see eight columns of data:
// MAGIC * **product_id** (*string*) Unique product identifier.
// MAGIC * **category** (*string*): Product is either a tablet or a laptop.
// MAGIC * **brand** (*string*): Product brand name.
// MAGIC * **model** (*string*): Product model.
// MAGIC * **price** (*double*): Product price
// MAGIC * **processor** (*string*): Product processor.
// MAGIC * **size** (*string*): Product size in inches.
// MAGIC * **display** (*string*): Display aspect ratio.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Querying dataframes
// MAGIC Spark provides a number of DataFrame methods that are useful for querying and working with data.

// COMMAND ----------

// MAGIC %md
// MAGIC ### show(..)
// MAGIC
// MAGIC The `show(..)` method has three optional parameters:
// MAGIC * **n**: The number of records to print to the console. The default is **20**.
// MAGIC * **truncate**: If true, columns wider than 20 characters will be truncated. The default is **True**.
// MAGIC * **vertical**: If true, rows are printed vertically (one line per column value). The default is **False**.
// MAGIC
// MAGIC Take a look at the data in our `DataFrame` with the `show()` method.
// MAGIC
// MAGIC <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=show#pyspark.sql.DataFrame.show" target="_blank">Python Docs</a>
// MAGIC
// MAGIC <a href="https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.Dataset" target="_blank">Scala Docs</a>

// COMMAND ----------


retailDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC In the cell above, change the show method's arguments to:
// MAGIC * print the first 5 records
// MAGIC * disable truncation
// MAGIC * print the first 10 records and disable truncation
// MAGIC
// MAGIC **Note:** The function `show(..)` is an **action** which triggers a job.

// COMMAND ----------

// MAGIC %md
// MAGIC ### display(..)
// MAGIC
// MAGIC The `show(..)` method is part of the core Spark API and simply prints the results to the console.
// MAGIC
// MAGIC The Azure Databricks Notebook environment has a more elegant, functional, and feature rich alternative.
// MAGIC
// MAGIC Instead of calling `show(..)` on an existing DataFrame, pass the DataFrame to the `display(..)` function:

// COMMAND ----------


display(retailDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### limit(..)
// MAGIC
// MAGIC Both `show(..)` and `display(..)` are **actions** that trigger jobs (though in slightly different ways).
// MAGIC
// MAGIC While `show(..)` has a parameter to control how many records are printed, `display(..)` does not.
// MAGIC
// MAGIC Address that difference with a transformation, `limit(..)`.
// MAGIC
// MAGIC While `show(..)`does print output, like many actions it does not actually return anything.
// MAGIC
// MAGIC Transformations like `limit(..)` return a **new** `DataFrame`.

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Here we create `limitedDF`, a new DataFrame that when displayed will only show the first 5 rows.

// COMMAND ----------


val limitedDF = retailDF.limit(5)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Nothing Happened
// MAGIC * Notice how "nothing" happened, that is, no job was triggered.  However, a new `DataFrame` object was returned.
// MAGIC * This is because you simply defined another transformation in the current string of transformations.
// MAGIC   0. Read in the parquet file (represented by **retailDF**).
// MAGIC   0. Limit those records to the first 5 (represented by **limitedDF**).
// MAGIC * It's not until an action is executed that a job is triggered and the data is processed.
// MAGIC
// MAGIC Induce a job by calling either the `show(..)` or the `display(..)` actions:

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC The following cell asks Spark to show up to 100 records and not truncate the columns. However, since `limitedDF` was limited to only 5 rows, only 5 rows will appear in the results.

// COMMAND ----------


limitedDF.show(100, false)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Display will attempt to show up to 1000 records, but in this case will only show the 5 in `limitedDF`.

// COMMAND ----------


display(limitedDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### count()
// MAGIC
// MAGIC Use the `count()` action to return the number of rows in `retailDF`.
// MAGIC
// MAGIC Take a look at the documentation to see how to use `count()`.
// MAGIC
// MAGIC <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=count#pyspark.sql.DataFrame.count" target = "_blank">Python Docs</a>
// MAGIC
// MAGIC <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset" target = "_blank">Scala Docs</a>

// COMMAND ----------


val total = retailDF.count()

println(s"Record Count: $total")

// COMMAND ----------

// MAGIC %md
// MAGIC ### select(..)
// MAGIC
// MAGIC In this case, only 5 out of the 8 columns are needed.
// MAGIC
// MAGIC The `Select` transformation can be used to return a new `dataframe` that contains only the columns that are needed.

// COMMAND ----------


val selectDF = retailDF.select("product_id", "category", "brand", "model", "price")

selectDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC Again, notice how the call to `select(..)` does not trigger a job. That's because `select(..)` is a transformation.
// MAGIC
// MAGIC Invoke the action `show(..)` and take a look at the result.

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC The following cell uses the DataFrame's `.show()` method. By default `.show()` will truncate the output.

// COMMAND ----------


selectDF.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC ### drop(..)
// MAGIC
// MAGIC **Note:** There are a lot of ways to accomplish the same task.
// MAGIC
// MAGIC Instead of using `select` to declare all the columns, use `drop(..)` to specify the columns to exclude.
// MAGIC
// MAGIC Produce the exact same result as the last exercise using `drop(..)`:

// COMMAND ----------


val droppedDF = retailDF.drop("processor", "size", "display")

droppedDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ### withColumnRenamed(..)
// MAGIC
// MAGIC There are many ways to rename columns of a DataFrame in Spark.
// MAGIC
// MAGIC `withColumnRenamed("oldName", "newName")` facilitates renaming columns one at a time.

// COMMAND ----------


droppedDF.withColumnRenamed("product_id", "prodID").printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ### withColumn(..)
// MAGIC
// MAGIC `withColumn` allows you to add new columns, or overwrite the values of existing columns using a column expression.
// MAGIC
// MAGIC Make a new column which equals double the `price` field. There are a few ways to do this.
// MAGIC
// MAGIC One is to import the `col` (column) function and apply it to the `price` column (recommended).

// COMMAND ----------


val doublePriceDF = droppedDF.withColumn("doublePrice", $"price" * 2)

doublePriceDF.show(3)

// COMMAND ----------

// MAGIC %md
// MAGIC Another way is to use Python Pandas syntax (i.e. `droppedDF["price"]`) as shown below.

// COMMAND ----------


droppedDF.withColumn("doublePrice", droppedDF("price") * 2).show(3)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Selecting and transforming columns
// MAGIC
// MAGIC Members of the `Column` class are objects that encompass more than the name of the column, but also column-level-transformations, such as sorting in a descending order.
// MAGIC
// MAGIC Below are examples of ways to create `Column` objects:

// COMMAND ----------


// Option A: Access a column from a known DataFrame
val columnA = retailDF("price")
println(columnA)

// Option B: Use the $ function to reference a named column (Scala Only, not available in Python)
val columnB = $"price"
println(columnB)

// Import from org.apache.spark.sql.sql.functions to make available a few additional options.
import org.apache.spark.sql.functions.{col, expr, lit}

// Option C: Use the col(..) function (this is identical to option B but available in both Scala and Python.)
val columnC = col("price")
println(columnC)

// Option D: Use the expr(..) function to create a column from an SQL Expression
val columnD = expr("price * 2")
println(columnD)

// Option E: Uses the lit(..) function to create a literal (constant) value.
val columnE = lit("abc")
println(columnE)

// COMMAND ----------

// MAGIC %md
// MAGIC The generally preferred syntax in Scala is: `$"price"`.
// MAGIC The generally preferred syntax in Python is: `col("price")`.
// MAGIC
// MAGIC Once the column object is created, it can be used to generate compound expressions:

// COMMAND ----------


import org.apache.spark.sql.functions.{round => sparkRound}

display(droppedDF.withColumn("Tax", sparkRound(($"price" * 1.1),2)))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This lesson discusses the import from `...sql.functions` as it relates to creating `Column` objects and using the `round` function.<br/>
// MAGIC Future lessons will introduce features available through the imported API.

// COMMAND ----------

// MAGIC %md
// MAGIC ### selectExpr(..)
// MAGIC
// MAGIC `selectExpr` allows you to select columns, rename columns, and create new columns all in one.

// COMMAND ----------


display(retailDF.selectExpr("product_id as prodID", "category", "brand","price", "price*2 as 2xPrice"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### filter(..)
// MAGIC Returns a new `DataFrame` containing only those records for which the specified condition is true.  For the argument, `filter(..)` expects either a column object or an SQL expression.
// MAGIC
// MAGIC Filter  all of the records where the price is less than $2000.

// COMMAND ----------


display(retailDF.filter($"price" >= 2000))

// COMMAND ----------


display(retailDF.filter("price >= 2000"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### groupBy(..)
// MAGIC The groupBy(..) function is one tool that can be used to aggregate data.  It does not return a `DataFrame` but rather:
// MAGIC * In Scala it returns a `RelationalGroupedDataset` object.
// MAGIC * In Python it returns a `GroupedData` object.
// MAGIC Each of these requires an aggregate function in order to return a `DataFrame`.  The `count( )` method can be used here to group products by the `brand` field.  Additionally, the `orderBy(..)` function can be used to sort the resulting `DataFrame` so it's easy to see which brand's products occur the most.
// MAGIC
// MAGIC Look at the Documentation to see the different methods that can be called on groupedData or RelationalGroupedDataset objects.
// MAGIC
// MAGIC <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.GroupedData" target="_blank">Python Docs</a>
// MAGIC
// MAGIC <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.RelationalGroupedDataset" target="_blank">Scala Docs</a>

// COMMAND ----------


display(retailDF.groupBy("brand")
  .count()
  .orderBy($"count".desc)
)

// COMMAND ----------

// MAGIC %md
// MAGIC ### take(n)
// MAGIC
// MAGIC Take returns a collection of the first `n` records to the driver.  The resulting collection can be itterated through to write as output, or to do some other work on items in the collection.

// COMMAND ----------


val items = retailDF.take(10)

for (i <- items) {
  println(i)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## collect(..)
// MAGIC
// MAGIC `collect( )` returns a collections containing all of the data to the driver.
// MAGIC
// MAGIC **WARNING** Calling `collect( )` on a large dataset is the easiest way to cause the driver to run out of memory. Be very careful when you use `collect( )`.
// MAGIC
// MAGIC Unless `collect( )` is required, and it is known that the driver can handle the data that will be returned, it is advisable to make a practice of using `take(n)`

// COMMAND ----------


val items = retailDF.groupBy("brand")
  .count
  .limit(40)
  .collect

for (i <- items) {
  println(i)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Sharing/Exporting Notebooks
// MAGIC
// MAGIC Walk through ways to share this notebook with your colleagues.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
