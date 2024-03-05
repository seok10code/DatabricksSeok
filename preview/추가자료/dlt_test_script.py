# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.table(
    name = "raw_table"
    comment = "Start streaming from source"
)
def streaming_bronze():
    return (spark.read.format("csv").load("dbfs:/FileStore/sw_test/sample_sw.csv").withColumn("velo_diff", col("velocity").lead().over(Window.partitionBy("vehicle_ID").orderBy('Datetime')))
#     spark.readStream.format("delta").load("path")
#     spark.readStream.format("cloudFiles")
#       .option("cloudFiles.format", "json")
#       .load("abfss://path/to/raw/data")
            
@dlt.table(
    name = "silver_table"
    comment = "Add lead_velo column that is the previous row values of velocity"
)
def streaming_silver():
  # Since we read the bronze table as a stream, this silver table is also
  # updated incrementally.
    return (dlt.read_stream("streaming_bronze")\
            .withColumn("lead_velo", lag("velocity"))\
            .over(Window.orderBy('Datetime'))
           )

@dlt.table(
    name = "gold_table"
    comment = "It's done"
)
def live_gold():
    return (dlt.read("streaming_silver")\
            .withColumn("velo_diff", 
                        when(col("lead_velo").isNull(), 0))\
                        .otherwise(col("velocity")-col("lead_velo"))
            
           )
