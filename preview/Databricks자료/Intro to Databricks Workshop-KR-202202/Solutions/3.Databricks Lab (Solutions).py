# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Querying Files with Dataframes
# MAGIC
# MAGIC Apache Spark&trade; and Databricks&reg; allow you to use DataFrames to query large data files.
# MAGIC
# MAGIC ## In this lesson you:
# MAGIC * Learn about Spark DataFrames.
# MAGIC * Query large files using Spark DataFrames.
# MAGIC * Visualize query results using charts.
# MAGIC
# MAGIC This lesson uses the `people-10m` data set, which is in Parquet format.
# MAGIC
# MAGIC The data is fictitious; in particular, the Social Security numbers are fake.

# COMMAND ----------

# MAGIC %run "./Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ###DataFrames
# MAGIC
# MAGIC Under the covers, DataFrames are derived from data structures known as Resilient Distributed Datasets (RDDs). RDDs and DataFrames are immutable distributed collections of data. Let's take a closer look at what some of these terms mean before we understand how they relate to DataFrames:
# MAGIC
# MAGIC * **Resilient**: They are fault tolerant, so if part of your operation fails, Spark  quickly recovers the lost computation.
# MAGIC * **Distributed**: RDDs are distributed across networked machines known as a cluster.
# MAGIC * **DataFrame**: A data structure where data is organized into named columns, like a table in a relational database, but with richer optimizations under the hood. 
# MAGIC
# MAGIC Without the named columns and declared types provided by a schema, Spark wouldn't know how to optimize the executation of any computation. Since DataFrames have a schema, they use the Catalyst Optimizer to determine the optimal way to execute your code.
# MAGIC
# MAGIC DataFrames were invented because the business community uses tables in a relational database, Pandas or R DataFrames, or Excel worksheets. A Spark DataFrame is conceptually equivalent to these, with richer optimizations under the hood and the benefit of being distributed across a cluster.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### DataFrames and SQL
# MAGIC
# MAGIC DataFrame syntax is more flexible than SQL syntax. Here we illustrate general usage patterns of SQL and DataFrames.
# MAGIC
# MAGIC Suppose we have a data set we loaded as a table called `myTable` and an equivalent DataFrame, called `df`.
# MAGIC We have three fields/columns called `col_1` (numeric type), `col_2` (string type) and `col_3` (timestamp type)
# MAGIC Here are basic SQL operations and their DataFrame equivalents. 
# MAGIC
# MAGIC Notice that columns in DataFrames are referenced by `col("<columnName>")`.
# MAGIC
# MAGIC | SQL                                         | DataFrame (Python)                    |
# MAGIC | ------------------------------------------- | ------------------------------------- | 
# MAGIC | `SELECT col_1 FROM myTable`                 | `df.select(col("col_1"))`             | 
# MAGIC | `DESCRIBE myTable`                          | `df.printSchema()`                    | 
# MAGIC | `SELECT * FROM myTable WHERE col_1 > 0`     | `df.filter(col("col_1") > 0)`         | 
# MAGIC | `..GROUP BY col_2`                          | `..groupBy(col("col_2"))`             | 
# MAGIC | `..ORDER BY col_2`                          | `..orderBy(col("col_2"))`             | 
# MAGIC | `..WHERE year(col_3) > 1990`                | `..filter(year(col("col_3")) > 1990)` | 
# MAGIC | `SELECT * FROM myTable LIMIT 10`            | `df.limit(10)`                        |
# MAGIC | `display(myTable)` (text format)            | `df.show()`                           | 
# MAGIC | `display(myTable)` (html format)            | `display(df)`                         |
# MAGIC
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You can also run SQL queries with the special syntax `spark.sql("SELECT * FROM myTable")`

# COMMAND ----------

# MAGIC %md
# MAGIC Use the `%fs` command to view the contents in **/mnt/training/dataframes/people-10m.parquet/**

# COMMAND ----------

# MAGIC %fs ls /mnt/training/dataframes/people-10m.parquet/

# COMMAND ----------

# MAGIC %md 
# MAGIC Read the parquet files and assign them to a DataFrame called `peopleDF`

# COMMAND ----------

peopleDF = spark.read.parquet("/mnt/training/dataframes/people-10m.parquet")

# COMMAND ----------

display(peopleDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at the schema with the `printSchema()` method. This tells you the field name, field type, and whether the column is nullable or not (default is true).

# COMMAND ----------

peopleDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Answer the following question:
# MAGIC > According to our data, which women were born after 1990?
# MAGIC
# MAGIC * Use the DataFrame `select` and `filter` methods. 
# MAGIC * Use `from pyspark.sql.functions import year` to extract the year from **birthDate**
# MAGIC
# MAGIC *Hint: `.filter(year("birthDate") > "1990")`*

# COMMAND ----------

from pyspark.sql.functions import year
display(
  peopleDF 
    .select("firstName","middleName","lastName","birthDate","gender") 
    .filter("gender = 'F'") 
    .filter(year("birthDate") > "1990")
)

# COMMAND ----------

# MAGIC %md
# MAGIC How many women were named Mary in each year?
# MAGIC * Use the DataFrame `orderBy` and `groupBy` methods. 
# MAGIC
# MAGIC *Hint: Create a new column called **birthYear** from **birthDate** by using `peopleDF.select(year("birthDate").alias("birthYear")`*

# COMMAND ----------

marysDF = (peopleDF.select(year("birthDate").alias("birthYear")) 
  .filter("firstName = 'Mary' ") 
  .filter("gender = 'F' ") 
  .orderBy("birthYear") 
  .groupBy("birthYear") 
  .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a line graph that displays the number of Marys born each year

# COMMAND ----------

display(marysDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Compare popularity of any two names from 1990 and create a line graph that displays the count of both names for each year.
# MAGIC
# MAGIC * Use `from pyspark.sql.functions import col` to specify DataFrame columns
# MAGIC
# MAGIC *Hint: `.filter((col("firstName") == 'Chris') | (col("firstName") == 'Noel'))`*

# COMMAND ----------

from pyspark.sql.functions import col

filteredDF = (peopleDF 
  .select(year("birthDate").alias("birthYear"), "firstName") 
  .filter((col("firstName") == 'Chris') | (col("firstName") == 'Noel'))
  .filter("gender == 'M' ") 
  .filter(year("birthDate") > 1990) 
  .orderBy("birthYear") 
  .groupBy("birthYear", "firstName") 
  .count()
)

# COMMAND ----------

display(filteredDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Temporary Views
# MAGIC
# MAGIC In DataFrames, <b>temporary views</b> are used to make the DataFrame available to SQL, and work with SQL syntax seamlessly.
# MAGIC
# MAGIC A temporary view gives you a name to query from SQL, but unlike a table it exists only for the duration of your Spark Session. As a result, the temporary view will not carry over when you restart the cluster or switch to a new notebook. It also won't show up in the Data button on the menu on the left side of a Databricks notebook which provides easy access to databases and tables.
# MAGIC
# MAGIC Create a temporary view containing the same data.

# COMMAND ----------

peopleDF.createOrReplaceTempView("People10M")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC View the contents of temporary view, using `spark.sql("SELECT * FROM myTable")` where firstName = "Chris"

# COMMAND ----------

display(spark.sql("SELECT * FROM  People10M WHERE firstName = 'Chris' "))

# COMMAND ----------

# MAGIC %md
# MAGIC View the contents of temporary view, using `%sql` where firstName = "Chris"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM People10M WHERE firstName = 'Chris'

# COMMAND ----------

# MAGIC %md
# MAGIC **Note**: In Databricks, `%sql` is the equivalent of `display()` combined with `spark.sql()`:
