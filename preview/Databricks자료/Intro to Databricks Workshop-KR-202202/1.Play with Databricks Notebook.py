# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # ![](https://redislabs.com/wp-content/uploads/2016/12/lgo-partners-databricks-125x125.png) 데이터브릭스 시작하기
# MAGIC 데이터브릭스는 Apache Spark™의 제작자가 만든 **통합 분석 플랫폼**으로 데이터 준비, 탐색 및 분석과 머신러닝 어플리케이션의 디플로이까지 전체적인 머신러닝/AI 라이프사이클 구성을 하는데 데이터 엔지니어, 데이터 과학자, 분석가들이 같이 협업할 수 있는 공간을 제공합니다.
# MAGIC
# MAGIC 데이터브릭스의 노트북 환경을 통해 다양한 업무들을 협업하는 과정을 알아봅시다!

# COMMAND ----------

# DBTITLE 1,Markdown
# MAGIC %md
# MAGIC this is text
# MAGIC
# MAGIC this is `code`
# MAGIC
# MAGIC # this is a header
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 다양한 언어 지원
# MAGIC Additional Magic Commands allow for the execution of code in languages other than the notebook's default:
# MAGIC * **&percnt;python** 
# MAGIC * **&percnt;scala** 
# MAGIC * **&percnt;sql** 
# MAGIC * **&percnt;r** 

# COMMAND ----------

print("Hello Python!")

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC println("Hello Scala!")

# COMMAND ----------

# MAGIC %r
# MAGIC
# MAGIC print("Hello R!", quote=FALSE)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select "Hello SQL!"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Magic Command: &percnt;sh
# MAGIC **&percnt;sh** 클러스터의 드라이버 노드에서 sh 커맨드를 수행합니다

# COMMAND ----------

# MAGIC %sh 
# MAGIC
# MAGIC ps | grep 'java'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Magic Command: &percnt;run
# MAGIC * You can run a notebook from another notebook by using the Magic Command **%run** 
# MAGIC * All variables & functions defined in that other notebook will become available in your current notebook
# MAGIC
# MAGIC For example, The following cell should fail to execute because the variable `username` has not yet been declared:

# COMMAND ----------

print("username: " + username)

# COMMAND ----------

# MAGIC %md
# MAGIC But we can declare it and a handful of other variables and functions buy running this cell:

# COMMAND ----------

# MAGIC %run "./Setup"

# COMMAND ----------

print("username: " + username)

# COMMAND ----------

# MAGIC %md ## Widget 을 이용한 동적인 변수 활용
# MAGIC
# MAGIC Databrick utilites (e.g. `dbutils`) provides functionality for many common tasks within Databricks notebooks: 
# MAGIC https://docs.databricks.com/dev-tools/databricks-utils.html
# MAGIC
# MAGIC One useful feature is "Widgets" that allow you to dynamically program within your notebooks: https://docs.databricks.com/notebooks/widgets.html

# COMMAND ----------

#Uncomment this to remove all widgets
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown("dropdown_widget", "1", [str(x) for x in range(1, 4)])

# COMMAND ----------

print("The current value of the dropdown_widget is:", dbutils.widgets.get("dropdown_widget"))

# COMMAND ----------

dbutils.widgets.text("text_widget","Hello World!")
#print(dbutils.widgets.get("text_widget"))

# COMMAND ----------

# DBTITLE 1,Widgets & displayHTML(..)
dbutils.widgets.combobox("greeting","Hi", ["Hi", "Hey", "Hello"], "Oh,")
dbutils.widgets.text("name", "Anonymous", "Your name")

# COMMAND ----------

displayHTML("<h2>" + dbutils.widgets.get("greeting") + " " + dbutils.widgets.get("name") + ", Welcome to Databricks!</h2>")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Databricks File System - DBFS
# MAGIC * DBFS is a layer over a cloud-based object store
# MAGIC * Files in DBFS are persisted to the object store
# MAGIC * The lifetime of files in the DBFS are **NOT** tied to the lifetime of our cluster
# MAGIC * Mounting other object stores into DBFS gives Databricks users access via the file system (one of many techniques for pulling data into Spark)
# MAGIC
# MAGIC ### Databricks Utilities - dbutils
# MAGIC * You can access the DBFS through the Databricks Utilities class (and other file IO routines).
# MAGIC * An instance of DBUtils is already declared for us as `dbutils`.
# MAGIC * For in-notebook documentation on DBUtils you can execute the command `dbutils.help()`.
# MAGIC
# MAGIC ### Magic Command: &percnt;fs
# MAGIC **&percnt;fs** is a wrapper around `dbutils.fs`, thus `dbutils.fs.ls("/databricks-datasets")` is equivalent to running `%fs ls /databricks-datasets`

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Exploration

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets

# COMMAND ----------

# DBTITLE 1,Create a dataframe
datapath = "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"
diamondsDF = spark.read.format("csv")\
              .option("header","true")\
              .option("inferschema","true")\
              .load(datapath)

# COMMAND ----------

# DBTITLE 1,display(..) 함수를 통한 inline 시각화 
# MAGIC %md
# MAGIC The `display(..)` command is overloaded with a lot of other capabilities:
# MAGIC * Presents up to 1000 records.
# MAGIC * Exporting data as CSV.
# MAGIC * Rendering a multitude of different graphs.
# MAGIC * Rendering geo-located data on a world map.
# MAGIC
# MAGIC And as we will see later, it is also an excellent tool for previewing our data in a notebook.

# COMMAND ----------

display(diamondsDF)
#download option (not recommended more than 1 mn rows)

# COMMAND ----------

# Different functionality within the cell
# Plotting and options
display(diamondsDF)

# COMMAND ----------

# DBTITLE 1,Use Your Favorite Visualizations
# MAGIC %md
# MAGIC Databricks comes with built-in [visualization tools](https://docs.databricks.com/user-guide/visualizations/index.html) that support many popular libraries such as:
# MAGIC - Matplotlib
# MAGIC - ggplot
# MAGIC - Plotly
# MAGIC - Bokeh
# MAGIC - HTML, D3, SVG
# MAGIC
# MAGIC Databricks supports any BI tool that can connect with JDBC / ODBC such as [Tableau](https://docs.databricks.com/user-guide/bi/tableau.html#tableau), [PowerBI](https://docs.databricks.com/user-guide/bi/power-bi.html), [Alteryx](https://docs.databricks.com/user-guide/bi/alteryx.html), [Looker](https://docs.databricks.com/user-guide/bi/looker.html), [SQL Workbench](https://docs.databricks.com/user-guide/bi/workbenchj.html), etc.
# MAGIC
# MAGIC You can leverage popular 3rd party plotting libraries for Python and R

# COMMAND ----------

# Use your favorite charting library
import numpy as np
import matplotlib.pyplot as plt

points, zorder1, zorder2 = 500, 10, 5
x = np.linspace(0, 1, points)
y = np.sin(4 * np.pi * x) * np.exp(-5 * x)

fig, ax = plt.subplots()

ax.fill(x, y, zorder=zorder1)
ax.grid(True, zorder=zorder2)
plt.show()
display() # Databricks display
plt.close() # Ensure you close it

# COMMAND ----------

# DBTITLE 1,Create a table programmatically
diamondsDF.createOrReplaceTempView("koalas_fmi")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS koalas_fmi
# MAGIC   USING csv
# MAGIC   OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT cut FROM diamonds_table_fmi

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM diamonds_table_fmi WHERE cut = "Fair"
# MAGIC LIMIT 10

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC # Machine Learning
# MAGIC
# MAGIC You can build an entire machine learning pipeline on Databricks.
# MAGIC
# MAGIC In additional to Spark ML, Databricks also supports popular machine learning and deep learning libraries including:
# MAGIC - sci-kit learn
# MAGIC - Pytorch
# MAGIC - XGBoost
# MAGIC - Keras
# MAGIC - Tensorflow
# MAGIC - Datarobot
# MAGIC - And more!

# COMMAND ----------

# MAGIC %md 
# MAGIC "Notebook-scoped libraries" : Databricks Runtime위에 특정 노트북용 라이브러리 설치
# MAGIC * Notebook-scoped Libraries [Blog](https://databricks.com/blog/2019/01/08/introducing-databricks-library-utilities-for-notebooks.html) | [Databricks Utilities]()
# MAGIC * For Machine Learning Runtime, use Conda or Pip for environment management: [Notebook Scoped Python Libraries](https://docs.databricks.com/notebooks/notebooks-python-libraries.html)

# COMMAND ----------

# Databricks Runtime에 현재 scipy 버전 확인
import pkg_resources
pkg_resources.get_distribution('scipy').version

# COMMAND ----------

# cluster 에 attach 된 notebook session 에 추가 library가 설치된 것이 있는지 확인
dbutils.library.list()

# COMMAND ----------

dbutils.library.installPyPI('scipy','1.6.3')
dbutils.library.restartPython()
dbutils.library.list()

# 이거말고 %pip install 을 쓰라고 함. 

# COMMAND ----------

# MAGIC %md
# MAGIC # Collaboration & Security
# MAGIC
# MAGIC 기타 데이터브릭스 노트북 협업 관련 기능들:
# MAGIC - Commenting
# MAGIC - Revision history
# MAGIC - Coediting like Google Docs
# MAGIC - Fine Grain Access Controls for Workspace, Notebook, Clusters, Jobs, and Tables
# MAGIC
# MAGIC ## Advanced features and product offerings
# MAGIC - Cluster Management
# MAGIC - Jobs (automatic workloads)
# MAGIC - Importing third-party libraries
# MAGIC - Structured Streaming
# MAGIC - Delta
# MAGIC - MLflow (full ML lifecyle)
# MAGIC - GraphX and GraphFrames
# MAGIC - Deep Learning

# COMMAND ----------

# MAGIC %md 
# MAGIC ###그래도 난 다른 노트북/개발환경이 좋다면!? 
# MAGIC * [Databricks Connect for any IDE](https://docs.databricks.com/dev-tools/databricks-connect.html)
# MAGIC * [Jupyter Labs (coming soon!)](https://databricks.com/blog/2019/12/03/jupyterlab-databricks-integration-bridge-local-and-remote-workflows.html)
# MAGIC * [Hosted R Studio](https://docs.databricks.com/spark/latest/sparkr/rstudio.html)

# COMMAND ----------


