# Databricks notebook source
# MAGIC %md
# MAGIC # Titanic: Machine Learning from Disaster (Kaggle)
# MAGIC ### A basic example to illustrate the usage of PySpark ML
# MAGIC
# MAGIC <a href="https://www.kaggle.com/c/titanic/">Titanic competition on Kaggle</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Agenda for this session
# MAGIC
# MAGIC Today we will walk through a whole data flow for a malware prediction use case, based on The Titanic data on the [Kaggle platform](https://www.kaggle.com/c/titanic)
# MAGIC The data flow will contain common usage for Data Engineers, Data Scientists, and Business Analysts. 
# MAGIC
# MAGIC ####The topics we will cover:
# MAGIC
# MAGIC * Data Engineering & Analysis
# MAGIC   - Extracting Data
# MAGIC   - Exploratory Analysis
# MAGIC   - Cleaning Data
# MAGIC * Data Science
# MAGIC   - ML Workflows
# MAGIC   - Spark MLlib
# MAGIC   - Training and Tuning Models
# MAGIC * Deployment
# MAGIC   - Overview of Model Serving Options
# MAGIC   - Serving in Batch and Streams
# MAGIC   - Serving via Web Service

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) A gentle introduction to Apache Spark on Azure Databricks
# MAGIC
# MAGIC ** Welcome to Azure Databricks! **
# MAGIC
# MAGIC This notebook is intended to be the first step in your process to learn more about how to best use Apache Spark on Azure Databricks together. We'll be walking through the core concepts, the fundamental abstractions, and the tools at your disposal. 
# MAGIC
# MAGIC First, it's worth defining Azure Databricks. Azure Databricks is a managed platform for running Apache Spark - that means that you do not have to learn complex cluster management concepts nor perform tedious maintenance tasks to take advantage of Spark. 
# MAGIC
# MAGIC Azure Databricks also provides a host of features to help its users be more productive with Spark. It's a point and click platform for those that prefer a user interface like data scientists or data analysts. However, this UI is accompanied by a sophisticated API for those that want to automate aspects of their data workloads with automated jobs. To meet the needs of enterprises, Azure Databricks also includes features such as role-based access control, integration with Azure Active Directory and other intelligent optimizations that not only improve usability for users but also reduce costs and complexity for administrators.
# MAGIC
# MAGIC Azure Databricks is Unified Analytics Platform for Data Engineers, Data Scientists and Analysis  
# MAGIC
# MAGIC ![UAP](https://databricks.com/wp-content/themes/databricks/assets/images/uap/marchitecture.png)
# MAGIC
# MAGIC ## Databricks Terminology
# MAGIC
# MAGIC Databricks has key concepts that are worth understanding. You'll notice that many of these line up with the links and icons that you'll see on the left side. These together define the fundamental tools that Databricks provides to you as an end user. They are available both in the web application UI as well as the REST API.
# MAGIC
# MAGIC -   ****Workspaces****
# MAGIC     -   Workspaces allow you to organize all the work that you are doing on Databricks. Like a folder structure in your computer, it allows you to save ****notebooks**** and ****libraries**** and share them with other users. Workspaces are not connected to data and should not be used to store data. They're simply for you to store the ****notebooks**** and ****libraries**** that you use to operate on and manipulate your data with.
# MAGIC -   ****Notebooks****
# MAGIC     -   Notebooks are a set of any number of cells that allow you to execute commands. Cells hold code in any of the following languages: `Scala`, `Python`, `R`, `SQL`, or `Markdown`. Notebooks have a default language, but each cell can have a language override to another language. This is done by including `%[language name]` at the top of the cell. For instance `%python`. We'll see this feature shortly.
# MAGIC     -   Notebooks need to be connected to a ****cluster**** in order to be able to execute commands however they are not permanently tied to a cluster. This allows notebooks to be shared via the web or downloaded onto your local machine.
# MAGIC     -   Here is a demonstration video of [Notebooks](http://www.youtube.com/embed/MXI0F8zfKGI).
# MAGIC     -   ****Dashboards****
# MAGIC         -   ****Dashboards**** can be created from ****notebooks**** as a way of displaying the output of cells without the code that generates them. 
# MAGIC     - ****Notebooks**** can also be scheduled as ****jobs**** in one click either to run a data pipeline, update a machine learning model, or update a dashboard.
# MAGIC -   ****Libraries****
# MAGIC     -   Libraries are packages or modules that provide additional functionality that you need to solve your business problems. These may be custom written Scala or Java jars; python eggs or custom written packages. You can write and upload these manually or you may install them directly via package management utilities like pypi or maven.
# MAGIC -   ****Tables****
# MAGIC     -   Tables are structured data that you and your team will use for analysis. Tables can exist in several places. Tables can be stored on Amazon S3, they can be stored on the cluster that you're currently using, or they can be cached in memory. [For more about tables see the documentation](https://docs.cloud.databricks.com/docs/latest/databricks_guide/index.html#02%20Product%20Overview/07%20Tables.html).
# MAGIC -   ****Clusters****
# MAGIC     -   Clusters are groups of computers that you treat as a single computer. In Databricks, this means that you can effectively treat 20 computers as you might treat one computer. Clusters allow you to execute code from ****notebooks**** or ****libraries**** on set of data. That data may be raw data located on S3 or structured data that you uploaded as a ****table**** to the cluster you are working on. 
# MAGIC     - It is important to note that clusters have access controls to control who has access to each cluster.
# MAGIC     -   Here is a demonstration video of [Clusters](http://www.youtube.com/embed/2-imke2vDs8).
# MAGIC -   ****Jobs****
# MAGIC     -   Jobs are the tool by which you can schedule execution to occur either on an already existing ****cluster**** or a cluster of its own. These can be ****notebooks**** as well as jars or python scripts. They can be created either manually or via the REST API.
# MAGIC     -   Here is a demonstration video of [Jobs](<http://www.youtube.com/embed/srI9yNOAbU0).
# MAGIC -   ****Apps****
# MAGIC     -   Apps are third party integrations with the Databricks platform. These include applications like Tableau.
# MAGIC
# MAGIC
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png)Azure Databricks Architecture
# MAGIC
# MAGIC Before proceeding with our example, let's see an overview of the Apache Spark architecture. As mentioned before, Apache Spark allows you to treat many machines as one machine and this is done via a master-worker type architecture where there is a `driver` or master node in the cluster, accompanied by `worker` nodes. The master sends work to the workers and either instructs them to pull to data from memory or from disk (or from another data source).
# MAGIC
# MAGIC The diagram below shows an example Apache Spark cluster, basically there exists a Driver node that communicates with executor nodes. Each of these executor nodes have slots which are logically like execution cores. 
# MAGIC
# MAGIC ![spark-architecture](https://training.databricks.com/databricks_guide/gentle_introduction/videoss_logo.png)
# MAGIC
# MAGIC The Driver sends Tasks to the empty slots on the Executors when work has to be done:
# MAGIC
# MAGIC ![spark-architecture](https://training.databricks.com/databricks_guide/gentle_introduction/spark_cluster_tasks.png)
# MAGIC
# MAGIC
# MAGIC
# MAGIC Furthermore, Spark allows two distinct kinds of operations by the user. There are **transformations** and there are **actions**.
# MAGIC
# MAGIC ### Transformations
# MAGIC
# MAGIC Transformations are operations that will not be completed at the time you write and execute the code in a cell - they will only get executed once you have called a **action**. An example of a transformation might be to convert an integer into a float or to filter a set of values.
# MAGIC
# MAGIC ### Actions
# MAGIC
# MAGIC Actions are commands that are computed by Spark right at the time of their execution. They consist of running all of the previous transformations in order to get back an actual result. An action is composed of one or more jobs which consists of tasks that will be executed by the workers in parallel where possible
# MAGIC
# MAGIC Here are some simple examples of transformations and actions. Remember, these **are not all** the transformations and actions - this is just a short sample of them. We'll get to why Apache Spark is designed this way shortly!
# MAGIC
# MAGIC ![transformations and actions](http://training.databricks.com/databricks_guide/gentle_introduction/trans_and_actions.png)
# MAGIC
# MAGIC
# MAGIC Now we've seen that Spark consists of actions and transformations. Let's talk about why that's the case. The reason for this is that it gives a simple way to optimize the entire pipeline of computations as opposed to the individual pieces. This makes it exceptionally fast for certain types of computation because it can perform all relevant computations at once. Technically speaking, Spark `pipelines` this computation which we can see in the image below. This means that certain computations can all be performed at once (like a map and a filter) rather than having to do one operation for all pieces of data then the following operation.
# MAGIC
# MAGIC ![transformations and actions](http://training.databricks.com/databricks_guide/gentle_introduction/pipeline.png)
# MAGIC
# MAGIC Apache Spark can also keep results in memory as opposed to other frameworks that immediately write to disk after each task.
# MAGIC
# MAGIC
# MAGIC ## A Worked Example of Transformations and Actions
# MAGIC
# MAGIC To illustrate all of these architectural and most relevantly **transformations** and **actions** - let's go through a more thorough example, this time using `DataFrames` and a csv file. 
# MAGIC
# MAGIC The DataFrame and SparkSQL work almost exactly as we have described above, we're going to build up a plan for how we're going to access the data and then finally execute that plan with an action. We'll see this process in the diagram below. We go through a process of analyzing the query, building up a plan, comparing them and then finally executing it.
# MAGIC
# MAGIC ![Spark Query Plan](http://training.databricks.com/databricks_guide/gentle_introduction/query-plan-generation.png)
# MAGIC
# MAGIC While we won't go too deep into the details for how this process works, you can read a lot more about this process on the [Databricks blog](https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html). For those that want a more information about how Apache Spark goes through this process, I would definitely recommend that post!

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Azure Databricks Runtime & Spark clusters
# MAGIC
# MAGIC Azure Databricks is designed for Azure! This means:
# MAGIC * Decoupling Storage and Compute
# MAGIC * Ephemerial Clusters
# MAGIC * Multiple Clusters
# MAGIC * Autoscaling / Serverless
# MAGIC
# MAGIC ####Azure Databricks Clusters:
# MAGIC
# MAGIC * Clusters Spin up in minutes (~5min)!
# MAGIC * Two types of clusters:
# MAGIC   * Interactive (shared)
# MAGIC   * Job Clusters
# MAGIC * Interactive clusters can also be Azure Databricks Serverless Pools
# MAGIC * Cluster ACLs
# MAGIC * SQL Endpoints (JDBC/ODBC) for Power BI, Tableau, etc.
# MAGIC
# MAGIC ####Serverless
# MAGIC * Driver Fault Isolation
# MAGIC * Preemption
# MAGIC * Query Watch Dog
# MAGIC * SQL Data ACLs 
# MAGIC * Optimal Spark Version
# MAGIC * Optimal Spark configuration for sharing
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Azure Databricks Workspace & Notebooks
# MAGIC
# MAGIC * Interactive notebooks with support for multiple languages (SQL, Python, R and Scala)
# MAGIC * Real-time collaboration
# MAGIC * Notebook revision history
# MAGIC * One-click visualizations	
# MAGIC * Workspace ACLs
# MAGIC * Library Management
# MAGIC
# MAGIC
# MAGIC ![Clusters](https://docs.azuredatabricks.net/_images/create-notebook.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Azure Databricks Performance
# MAGIC
# MAGIC ####Azure Databricks Runtime Optimizations
# MAGIC
# MAGIC * Multiple versions of Spark (ephemerial Clusters)
# MAGIC * Fine grained data controls
# MAGIC * Optimized resource sharing
# MAGIC * Fault isolations of shared resources
# MAGIC * Data skipping
# MAGIC * Runtime optimizations during joins and filters
# MAGIC * Rapid release cycles
# MAGIC * Autoscaling
# MAGIC
# MAGIC ####Performance advantages:
# MAGIC
# MAGIC * Up to 5x faster than Vanilla Spark over AWS
# MAGIC * Databricks IO Cache vs classic RDD cache
# MAGIC * Seamless integration with Azure ecosystem
# MAGIC
# MAGIC ![AzureDatabricks-benchmarking](https://nachochurndata.blob.core.windows.net/images/Picture1.png)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Azure Databricks Data interfaces
# MAGIC
# MAGIC There are several key interfaces that you should understand when you go to use Spark.
# MAGIC
# MAGIC -   ****The Dataset****
# MAGIC     -   The Dataset is Apache Spark's newest distributed collection and can be considered a combination of DataFrames and RDDs. It provides the typed interface that is available in RDDs while providing a lot of conveniences of DataFrames. It will be the core abstraction going forward.
# MAGIC -   ****The DataFrame****
# MAGIC     -   The DataFrame is collection of distributed `Row` types. These provide a flexible interface and are similar in concept to the DataFrames you may be familiar with in python (pandas) as well as in the R language.
# MAGIC -   ****The RDD (Resilient Distributed Dataset)****
# MAGIC     -   Apache Spark's first abstraction was the RDD or Resilient Distributed Dataset. Essentially it is an interface to a sequence of data objects that consist of one or more types that are located across a variety of machines in a cluster. RDD's can be created in a variety of ways and are the "lowest level" API available to the user. While this is the original data structure made available, new users should focus on Datasets as those will be supersets of the current RDD functionality.

# COMMAND ----------

# MAGIC %md 
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png)Azure Databricks and Apache Spark Help Resources
# MAGIC
# MAGIC Azure Databricks comes with a variety of tools to help you learn how to use Azure Databricks and Apache Spark effectively. Databricks holds the greatest collection of Apache Spark documentation available anywhere on the web. There are two fundamental sets of resources that we make available: resources to help you learn how to use Apache Spark and Azure Databricks and resources that you can refer to if you already know the basics.
# MAGIC
# MAGIC To access these resources at any time, click the question mark button at the top right-hand corner. 
# MAGIC
# MAGIC ![img](https://training.databricks.com/databricks_guide/gentle_introduction/help_menu.png)
# MAGIC
# MAGIC -   ****The Azure Databricks Documentation****
# MAGIC     -   [The Azure Databricks documentation](https://docs.azuredatabricks.net/index.html) is the definitive reference for you and your team once you've become accustomed to using and leveraging Apache Spark. It allows for quick reference of common Azure Databricks and Spark APIs with snippets of sample code.
# MAGIC     -   The Guide also includes a series of tutorials (including this one!) that provide a more guided introduction to a given topic.
# MAGIC -   ****The Apache Spark Documentation****
# MAGIC     -   [The Apache Spark open source documentation](http://spark.apache.org/docs/latest/sql-programming-guide.html) is also made available for quick and simple search if you need to dive deeper into some of the internals of Apache Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Step
# MAGIC
# MAGIC [Extracting Data]($./01-Extracting-Data)

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
