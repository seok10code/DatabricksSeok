# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Koalas vs Pandas: The Ultimate Showdown Demo 
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/databricks/koalas/master/Koalas-logo.png" width="220"/>
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC ## In this Demo: 
# MAGIC * Basics of Koalas 
# MAGIC * Why we should use Koalas for Big Data with an example

# COMMAND ----------

# DBTITLE 1,Install Koalas using pip (or via the Clusters page)
# MAGIC %sh
# MAGIC pip install koalas

# COMMAND ----------

# DBTITLE 1,Import Koalas (and other libraries)
import pandas as pd
import numpy as np
import databricks.koalas as ks
from pyspark.sql import SparkSession

# COMMAND ----------

# DBTITLE 1,Create a pandas DataFrame
numbers = np.arange(0,6)
pdf = pd.DataFrame(np.random.randn(6, 4), index=numbers, columns=list('ABCD'))

pdf

# COMMAND ----------

# DBTITLE 1,A pandas DataFrame can be converted to a Koalas DataFrame
kdf = ks.from_pandas(pdf)

# kdf = ks.DataFrame(pdf)

# COMMAND ----------

type(kdf)

# COMMAND ----------

# DBTITLE 1,It looks and behaves the same as a pandas DataFrame too!
kdf

# COMMAND ----------

kdf.head()

# COMMAND ----------

kdf.describe()

# COMMAND ----------

# DBTITLE 1,Getting Data in/out: CSV
kdf.to_csv('fmi_foo.csv')
ks.read_csv('fmi_foo.csv').head(10)

# COMMAND ----------

# MAGIC %sh ls /dbfs | grep fmi*

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Let's do Some Natural Science 
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn%3AANd9GcSEZHbOBUglDoCu_k2x12lH6ZKA1fpVJx1wxFFt1fTkj2b-sd-r" width="420"/>
# MAGIC </div>
# MAGIC
# MAGIC Let's dig in a little deeper with a real dataset. 
# MAGIC
# MAGIC We will be looking at a dataset containing the occurrence of flora and fauna species (biodiversity) in the Netherlands on a 5 x 5 km scale. 
# MAGIC
# MAGIC Data is available from Observation.org, Nature data from the Netherlands.
# MAGIC
# MAGIC
# MAGIC ### Agenda
# MAGIC * Load Data
# MAGIC * Exploratory Data Analysis
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load full dataset using pandas
import pandas as pd
import glob

pandas_full_df = pd.concat([pd.read_csv(f) for f in glob.glob('/dbfs/koalas-demo-sais19eu/nature-data-nl.csv/*.csv')], ignore_index=True)
pandas_full_df.head()

# NOTE: This cell takes 10 mins and gets an OOM error

# COMMAND ----------

# DBTITLE 1,Load sample of the dataset using pandas
pandas_df = pd.read_csv("/dbfs/koalas-demo-sais19eu/nature-data-nl.csv/part-00000-tid-1118289550353537968-a4c9e5f5-4389-4477-adf1-4fd673f87330-9-1-c000.csv")
pandas_df.head()

# COMMAND ----------

# DBTITLE 1,The pandas sample DataFrame contains 269,047 rows and 50 columns
pandas_df.shape

# COMMAND ----------

# DBTITLE 1,Load full dataset using Koalas
import databricks.koalas as ks

koalas_df = ks.read_csv("dbfs:/koalas-demo-sais19eu/nature-data-nl.csv/") 
koalas_df.head()

# COMMAND ----------

# MAGIC %md Let's load our dataset using Koalas and setting the `default_index_type` to be `distributed`. See [here](https://koalas.readthedocs.io/en/latest/user_guide/options.html) for more info.

# COMMAND ----------

ks.set_option('compute.default_index_type', 'distributed')

koalas_df = ks.read_csv("dbfs:/koalas-demo-sais19eu/nature-data-nl.csv/") 
koalas_df.head()

# COMMAND ----------

# DBTITLE 1,The Koalas DataFrame contains 26,859,363 rows and 50 columns
koalas_df.shape

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Exploratory Data Analysis

# COMMAND ----------

# Grab the column names
koalas_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis of Bee Numbers in the Netherlands
# MAGIC ### Honey bee *(genus: Apis)*
# MAGIC
# MAGIC In the [Netherlands](https://diamondsci.com/blog/greening-cities-can-help-save-bees/), bee populations have been declining over the last 70 years or so. However, Amsterdam is making large strides to make the city bee-friendly by planting native species in parks, as well as in patches alongside sidewalks and road verges. It also encourages developers to install green roofs on new buildings in an effort to green up the city and create an environment that is welcoming to bees.
# MAGIC
# MAGIC Let's analyze the occurences of bees in our Dutch dataset.
# MAGIC
# MAGIC # 🐝  + 🐨 

# COMMAND ----------

# DBTITLE 1,Filter down to the genus Apis
honey_bee_ks_df = koalas_df[koalas_df.genus == "Apis"]
honey_bee_ks_df.head()

# COMMAND ----------

honey_bee_ks_df.shape[0] # We have 5,190 instances of honey bee recordings in the dataset.

# COMMAND ----------

# DBTITLE 1,Between what dates do we have recordings of Honey Bee numbers?
honey_bee_ks_df["eventDate"].min(), honey_bee_ks_df["eventDate"].max()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We have observations from 1993 up until November 2018. For each year, let's see if we have recordings of bee numbers for each month.
# MAGIC
# MAGIC We can query our Koalas DataFrame using SQL.

# COMMAND ----------

ks.sql("""
SELECT year, COUNT(DISTINCT month) AS num_unique_months
FROM {honey_bee_ks_df}
GROUP BY year
ORDER BY year
""")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Note that prior to 2011, we have at least 2 or more months in each year where we do not have recordings of bee numbers. Let's filter our honey bee Koalas DataFrame to only those recordings after 2011.

# COMMAND ----------

honey_bee_filtered_ks_df = honey_bee_ks_df[honey_bee_ks_df['year'] >= 2011] 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plotting Dutch honey bee numbers over time
# MAGIC
# MAGIC Suppose we want to plot the national bee count per month from 2011 to November 2018. To do so, we're first going to combine our year and month columns to create a monthly datetime column.

# COMMAND ----------

honey_bee_filtered_ks_df['year'].value_counts(dropna=False)

# COMMAND ----------

honey_bee_filtered_ks_df['year'].value_counts(dropna=False).sort_values()

# COMMAND ----------

import matplotlib.pyplot as plt

plt.clf()
display(honey_bee_filtered_ks_df['year'].value_counts(dropna=False).sort_values().plot.bar(rot=25, title="Bar plot of bee sightings per year using Koalas DataFrame").figure)

# COMMAND ----------

# DBTITLE 1,What about Koalas or Pandas? 
koalasAndPandas=koalas_df.loc[(koalas_df['species'] == "Phascolarctos cinereus") | (koalas_df['species'] == "Ailuropoda melanoleuca")]
display(koalasAndPandas)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Since the data was taken from the Netherlands, I guess it makes sense we didn't find these adorable bears... 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://zootles.files.wordpress.com/2017/01/panda-koala.png" width="620"/>
# MAGIC </div>
