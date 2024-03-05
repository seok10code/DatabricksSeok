# Databricks notebook source
# MAGIC %md
# MAGIC ## Lazy Evaluation in Spark
# MAGIC Fundamental to Apache Spark are the notions that
# MAGIC * Transformations are **LAZY**
# MAGIC * Actions are **EAGER**
# MAGIC
# MAGIC We see this play out when we run multiple transformations back-to-back, and no job is triggered:

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.linalg import VectorUDT

# COMMAND ----------

# MAGIC %run "./Setup"

# COMMAND ----------

# MAGIC %md 
# MAGIC <img src="https://i.vimeocdn.com/portrait/18609368_640x640" alt="databricks" width="30"/> You can point to any folder and Databricks can read multiple files at once (no more looping!)

# COMMAND ----------

# MAGIC %fs ls "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"

# COMMAND ----------

# Use a schema to avoid the overhead of inferring the schema
schema = StructType(
  [
    StructField("project", StringType(), False),
    StructField("article", StringType(), False),
    StructField("requests", IntegerType(), False),
    StructField("bytes_served", LongType(), False)
  ]
)

parquetFile = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"

pagecountsDF = (spark                                          # Our SparkSession & Entry Point
  .read                                                    # DataFrameReader
  .schema(schema)                                          # DataFrameReader (config)
  .parquet(parquetFile)                                    # Transformation (initial)
  .where( "project = 'en'" )                               # Transformation
  .drop("bytes_served")                                    # Transformation
  .filter( col("article") != "Main_Page")                  # Transformation
  .filter( col("article") != "-")                          # Transformation
  .filter( col("article").startswith("Special:") == False) # Transformation
)
print(pagecountsDF)

# COMMAND ----------

display(pagecountsDF) # Action

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Catalyst Optimizer
# MAGIC
# MAGIC When chaining operations together in a dataframe - e.g. `df.select(...)` `.groupBy(...)` `.filter(...)` - you don’t have to worry about how to order operations because the underlying optimizer (**the Catalyst Optimizer**) helps determine the optimal order of execution for you. 

# COMMAND ----------

# DBTITLE 1,Stages of the Catalyst Optimizer
# MAGIC %md
# MAGIC
# MAGIC ![Catalyst](https://files.training.databricks.com/images/105/catalyst-diagram.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Optimized Logical Plan
# MAGIC
# MAGIC One of the many optimizations performed by the Catalyst Optimizer involves **rewriting our code**.
# MAGIC   
# MAGIC In this case, we will see **two examples** involving the rewriting of our filters.
# MAGIC
# MAGIC The first is an **innocent mistake** almost every new Spark developer makes.
# MAGIC
# MAGIC The second "mistake" is... well... **really bad** - but Spark can fix it.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Example #1: Innocent Mistake
# MAGIC
# MAGIC I don't want any project that starts with **en.zero**.
# MAGIC
# MAGIC There are **better ways of doing this**, as in it can be done with a single condition.
# MAGIC
# MAGIC But we will make **8 passes** on the data **with 8 different filters**.
# MAGIC
# MAGIC After every individual pass, we will **go back over the remaining dataset** to filter out the next set of records.

# COMMAND ----------

innocentDF = (spark.read.parquet("/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/")
  .filter( col("project") != "en.zero")
  .filter( col("project") != "en.zero.n")
  .filter( col("project") != "en.zero.s")
  .filter( col("project") != "en.zero.d")
  .filter( col("project") != "en.zero.voy")
  .filter( col("project") != "en.zero.b")
  .filter( col("project") != "en.zero.v")
  .filter( col("project") != "en.zero.q")
)
print("Final Count: {0:,}".format( innocentDF.count() ))

# COMMAND ----------

# MAGIC %md
# MAGIC We don't even have to execute the code to see what is **logically** or **physically** taking place under the hood.
# MAGIC
# MAGIC Here we can use the `explain(..)` command.

# COMMAND ----------

innocentDF.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Example #2: Bad Programmer
# MAGIC
# MAGIC This time we are going to do something **REALLY** bad...
# MAGIC
# MAGIC Even if the compiler combines these filters into a single filter, **we still have five different tests** for any column that doesn't have the value "whatever".

# COMMAND ----------

stupidDF = (spark.read.parquet("/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/")
  .filter( col("project") != "whatever")
  .filter( col("project") != "whatever")
  .filter( col("project") != "whatever")
  .filter( col("project") != "whatever")
  .filter( col("project") != "whatever")
)

stupidDF.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ** *Note:* ** *`explain(..)` is not the only way to get access to this level of detail...<br/>
# MAGIC We can also see it in the **Spark UI**. *

# COMMAND ----------

# MAGIC %md
# MAGIC ###Columnar Predicate Pushdown
# MAGIC
# MAGIC The Columnar Predicate Pushdown takes place when a filter can be pushed down to the original data source, such as a database server.
# MAGIC
# MAGIC In this example, we are going to compare `DataFrames` from two different sources:
# MAGIC * JDBC - where a predicate pushdown **WILL** take place.
# MAGIC * CSV - where a predicate pushdown will **NOT** take place.
# MAGIC
# MAGIC In each case, we can see evidence of the pushdown (or lack of it) in the **Physical Plan**.

# COMMAND ----------

# DBTITLE 1,Reading from JDBC
# MAGIC %md
# MAGIC
# MAGIC Working with a JDBC data source is significantly different than any of the other data sources.
# MAGIC * Configuration settings can be a lot more complex.
# MAGIC * Often required to "register" the JDBC driver for the target database.
# MAGIC * We have to juggle the number of DB connections.
# MAGIC * We have to instruct Spark how to partition the data.
# MAGIC
# MAGIC **NOTE:** The database is read-only for security reasons. For examples of writing via JDBC, see 
# MAGIC   * <a href="https://docs.databricks.com/spark/latest/data-sources/sql-databases.html" target="_blank">Connecting to SQL Databases using JDBC</a>
# MAGIC   * <a href="http://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases" target="_blank">JDBC To Other Databases</a>

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // Ensure that the driver class is loaded. 
# MAGIC // Seems to be necessary sometimes.
# MAGIC Class.forName("org.postgresql.Driver") 

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we can create a DataFrame via JDBC and then filter by gender.

# COMMAND ----------

jdbcURL = "jdbc:postgresql://54.213.33.240/training"

# Username and Password w/read-only rights
connProperties = {
  "user" : "training",
  "password" : "training"
}

ppExampleThreeDF = (spark.read.jdbc(
    url=jdbcURL,                  # the JDBC URL
    table="training.people_1m",   # the name of the table
    column="id",                  # the name of a column of an integral type that will be used for partitioning
    lowerBound=1,                 # the minimum value of columnName used to decide partition stride
    upperBound=1000000,           # the maximum value of columnName used to decide partition stride
    numPartitions=8,              # the number of partitions/connections
    properties=connProperties     # the connection properties
  )
  .filter(col("gender") == "M")   # Filter the data by gender
)

# COMMAND ----------

# MAGIC %md
# MAGIC With the `DataFrame` created, we can ask Spark to `explain(..)` the **Physical Plan**.
# MAGIC
# MAGIC What we are looking for...
# MAGIC * is the lack of a **Filter** and
# MAGIC * the presence of a **PushedFilters** in the **Scan**

# COMMAND ----------

ppExampleThreeDF.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC This will make a little more sense if we **compare it to some examples** that don't push down the filter.

# COMMAND ----------

ppExampleFourCachedDF = (spark.read.jdbc(
    url=jdbcURL,                  # the JDBC URL
    table="training.people_1m",   # the name of the table
    column="id",                  # the name of a column of an integral type that will be used for partitioning
    lowerBound=1,                 # the minimum value of columnName used to decide partition stride
    upperBound=1000000,           # the maximum value of columnName used to decide partition stride
    numPartitions=8,              # the number of partitions/connections
    properties=connProperties     # the connection properties
  ))

(ppExampleFourCachedDF
  .cache()                        # cache the data
  .count())                       # materialize the cache

ppExampleFourFilteredDF = (ppExampleFourCachedDF
  .filter(col("gender") == "M"))  # Filter the data by gender

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have cached the data and THEN filtered it, we have eliminated the possibility to benefit from the predicate push down.
# MAGIC
# MAGIC So that it's easier to compare the two examples, we can re-print the physical plan for the previous example too.

# COMMAND ----------

print("****Example Three****\n")
ppExampleThreeDF.explain()

print("\n****Example Four****\n")
ppExampleFourFilteredDF.explain()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Caching

# COMMAND ----------

pagecountsDF.count()

# COMMAND ----------

pagecountsDF.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC If you re-run that command, it should take significantly less time.

# COMMAND ----------

pagecountsDF.count()

# COMMAND ----------

# Drop all tables/dataframes from cache
# !!! DO NOT RUN THIS ON A SHARED CLUSTER !!!
# YOU WILL CLEAR YOUR CACHE AND YOUR COWORKERS'
# spark.catalog.clearCache()

# Drop a specific table/dataframe from cache
pagecountsDF.unpersist()

# COMMAND ----------

pagecountsDF.createOrReplaceTempView("my_pagecounts_table")
spark.catalog.cacheTable("my_pagecounts_table")

pagecountsDF.count() # Action to trigger cache

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Patititoning
# MAGIC
# MAGIC When chaining operations together in a dataframe - e.g. `df.select(...)` `.groupBy(...)` `.filter(...)` - you don’t have to worry about how to order operations because the underlying optimizer (**the Catalyst Optimizer**) helps determine the optimal order of execution for you. 

# COMMAND ----------

display(pagecountsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Standard ETL
# MAGIC 1. Read the data in
# MAGIC 2. Balance the number of partitions to the number of slots
# MAGIC 3. *Possibly* cache the data 
# MAGIC 4. Adjust the `spark.sql.shuffle.partitions`
# MAGIC 5. Perform some basic ETL (convert strings to timestamp)
# MAGIC 6. *Possibly* re-cache the data if the ETL was costly

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# Use a schema to avoid the overhead of inferring the schema
# In the case of CSV/TSV it requires a full scan of the file.
schema = StructType(
  [
    StructField("timestamp", StringType(), False),
    StructField("site", StringType(), False),
    StructField("requests", IntegerType(), False)
  ]
)

fileName = "/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv"

# Create our initial DataFrame
initialDF = (spark.read
  .option("header", "true")
  .option("sep", "\t")
  .schema(schema)
  .csv(fileName)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Partitions vs Slots
# MAGIC
# MAGIC The Spark API uses the term **core** meaning a thread available for parallel execution. Here we refer to it as a **slot** to avoid confusion with the number of cores in the underlying CPU(s) to which there isn't necessarily an equal number.
# MAGIC
# MAGIC In most cases, if you created your cluster, you should know how many cores you have.
# MAGIC
# MAGIC However, to check programatically, you can use `SparkContext.defaultParallelism`
# MAGIC
# MAGIC For more information, see the doc <a href="https://spark.apache.org/docs/latest/configuration.html#execution-behavior" target="_blank">Spark Configuration, Execution Behavior</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Slots/Cores
# MAGIC
# MAGIC In most cases, if you created your cluster, you should know how many cores you have.
# MAGIC
# MAGIC However, to check programatically, you can use `SparkContext.defaultParallelism`
# MAGIC
# MAGIC For more information, see the doc <a href="https://spark.apache.org/docs/latest/configuration.html#execution-behavior" target="_blank">Spark Configuration, Execution Behavior</a>

# COMMAND ----------

cores = sc.defaultParallelism

print("You have {} cores, or slots.".format(cores))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partitions
# MAGIC
# MAGIC * How many partitions of data do I have?
# MAGIC * With that we have two subsequent questions:
# MAGIC   0. Why do I have that many?
# MAGIC   0. What is a partition?
# MAGIC
# MAGIC For the last question, a **partition** is a small piece of the total data set.
# MAGIC
# MAGIC Google defines it like this:
# MAGIC > the action or state of dividing or being divided into parts.
# MAGIC
# MAGIC If our goal is to process all our data (say 1M records) in parallel, we need to divide that data up.
# MAGIC
# MAGIC If I have 8 **slots** for parallel execution, it would stand to reason that I want 1M / 8 or 125,000 records per partition.

# COMMAND ----------

# MAGIC %md
# MAGIC Back to the first question, we can answer it by running the following command which
# MAGIC * takes the `initialDF`
# MAGIC * converts it to an `RDD`
# MAGIC * and then asks the `RDD` for the number of partitions

# COMMAND ----------

partitions = initialDF.rdd.getNumPartitions()
print("Partitions: {0:,}".format( partitions ))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * It is **NOT** coincidental that we have **8 slots** and **8 partitions**
# MAGIC * In Spark 2.0 a lot of optimizations have been added to the readers.
# MAGIC * Namely the readers looks at **the number of slots**, the **size of the data**, and makes a best guess at how many partitions **should be created**.
# MAGIC * You can actually double the size of the data several times over and Spark will still read in **only 8 partitions**.
# MAGIC * Eventually it will get so big that Spark will forgo optimization and read it in as 10 partitions, in that case.
# MAGIC
# MAGIC But 8 partitions and 8 slots is just too easy.
# MAGIC   * Let's read in another copy of this same data.
# MAGIC   * A parquet file that was saved in 5 partitions.
# MAGIC   * This gives us an excuse to reason about the **relationship between slots and partitions**

# COMMAND ----------

# Create our initial DataFrame. We can let it infer the 
# schema because the cost for parquet files is really low.
alternateDF = (spark.read
  .parquet("/mnt/training/wikipedia/pageviews/pageviews_by_second.parquet")
)

print("Partitions: {0:,}".format( alternateDF.rdd.getNumPartitions() ))

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have only 5 partitions we have to ask...
# MAGIC
# MAGIC What is going to happen when I perform and action like `count()` **with 8 slots and only 5 partitions?**

# COMMAND ----------

alternateDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC **Question #1:** Is it OK to let my code continue to run this way?
# MAGIC
# MAGIC **Question #2:** What if it was a **REALLY** big file that read in as **200 partitions** and we had **256 slots**?
# MAGIC
# MAGIC **Question #3:** What if it was a **REALLY** big file that read in as **200 partitions** and we had only **8 slots**, how long would it take compared to a dataset that has only 8 partitions?
# MAGIC
# MAGIC **Question #4:** Given the previous example (**200 partitions** vs **8 slots**) what are our options (given that we cannot increase the number of partitions)?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Every Slot/Core
# MAGIC
# MAGIC With some very few exceptions, you always want the number of partitions to be **a factor of the number of slots**.
# MAGIC
# MAGIC That way **every slot is used**.
# MAGIC
# MAGIC That is, every slot is being assigned a task.
# MAGIC
# MAGIC With 5 partitions & 8 slots we are **under-utilizing three of the eight slots**.
# MAGIC
# MAGIC With 9 partitions & 8 slots we just guaranteed our **job will take 2x** as long as it may need to.
# MAGIC * 10 seconds, for example, to process the first 8.
# MAGIC * Then as soon as one of the first 8 is done, another 10 seconds to process the last partition.

# COMMAND ----------

# MAGIC %md
# MAGIC ### More or Less Partitions?
# MAGIC
# MAGIC As a **general guideline** it is advised that each partition (when cached) is roughly around 200MB.
# MAGIC * Size on disk is not a good gauge. For example...
# MAGIC * CSV files are large on disk but small in RAM - consider the string "12345" which is 10 bytes compared to the integer 12345 which is only 4 bytes.
# MAGIC * Parquet files are highly compressed but uncompressed in RAM.
# MAGIC * In a relational database... well... who knows?
# MAGIC
# MAGIC The **200 comes from** the real-world-experience of Databricks's engineers and is **based largely on efficiency** and not so much resource limitations. 
# MAGIC
# MAGIC On an executor with a reduced amount of RAM (such as our CE JVMs with 6GB) you might need to lower that.
# MAGIC
# MAGIC For example, at 8 partitions (corresponding to our max number of slots) & 200MB per partition
# MAGIC * That will use roughly **1.5GB**
# MAGIC * We **might** get away with that on CE.
# MAGIC * If you have transformations that balloon the data size (such as Natural Language Processing) you are sure to run into problems.
# MAGIC
# MAGIC **Question:** If I read in my data and it comes in as 10 partitions should I...
# MAGIC * reduce my partitions down to 8 (1x number of slots)
# MAGIC * or increase my partitions up to 16 (2x number of slots)
# MAGIC
# MAGIC **Answer:** It depends on the size of each partition
# MAGIC * Read the data in. 
# MAGIC * Cache it. 
# MAGIC * Look at the size per partition.
# MAGIC * If you are near or over 200MB consider increasing the number of partitions.
# MAGIC * If you are under 200MB consider decreasing the number of partitions.
# MAGIC
# MAGIC The goal will **ALWAYS** be to use as few partitions as possible while maintaining at least 1 x number-of-slots.

# COMMAND ----------

# MAGIC %md
# MAGIC ##repartition(n) or coalesce(n)
# MAGIC
# MAGIC We have two operations that can help address this problem: `repartition(n)` and `coalesce(n)`.
# MAGIC
# MAGIC If you look at the API docs, `coalesce(n)` is described like this:
# MAGIC > Returns a new Dataset that has exactly numPartitions partitions, when fewer partitions are requested.<br/>
# MAGIC > If a larger number of partitions is requested, it will stay at the current number of partitions.
# MAGIC
# MAGIC If you look at the API docs, `repartition(n)` is described like this:
# MAGIC > Returns a new Dataset that has exactly numPartitions partitions.
# MAGIC
# MAGIC The key differences between the two are
# MAGIC * `coalesce(n)` is a **narrow** transformation and can only be used to reduce the number of partitions.
# MAGIC * `repartition(n)` is a **wide** transformation and can be used to reduce or increase the number of partitions.
# MAGIC
# MAGIC So, if I'm increasing the number of partitions I have only one choice: `repartition(n)`
# MAGIC
# MAGIC If I'm reducing the number of partitions I can use either one, so how do I decide?
# MAGIC * First off, `coalesce(n)` is a **narrow** transformation and performs better because it avoids a shuffle.
# MAGIC * However, `coalesce(n)` cannot guarantee even **distribution of records** across all partitions.
# MAGIC * For example, with `coalesce(n)` you might end up with **a few partitions containing 80%** of all the data.
# MAGIC * On the other hand, `repartition(n)` will give us a relatively **uniform distribution**.
# MAGIC * And `repartition(n)` is a **wide** transformation meaning we have the added cost of a **shuffle operation**.
# MAGIC
# MAGIC In our case, we "need" to go from 5 partitions up to 8 partitions - our only option here is `repartition(n)`. 

# COMMAND ----------

repartitionedDF = alternateDF.repartition(8)

print("Partitions: {0:,}".format( repartitionedDF.rdd.getNumPartitions() ))
