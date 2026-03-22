# Databricks notebook source
# COMMAND ----------


from pyspark.sql.functions import *
from pyspark.sql.types import *

bronze_path = "abfss://bronze@stspmobilitydev001.dfs.core.windows.net/sptrans/vehicle_positions"
silver_path = "abfss://silver@stspmobilitydev001.dfs.core.windows.net/sptrans/vehicle_positions"

df_bronze = spark.read.format("delta").load(bronze_path)

display(df_bronze)

df_silver = (
    df_bronze
    .withColumn("timestamp_api", col("timestamp_api").cast("timestamp"))
    .withColumn("ingestion_timestamp", col("ingestion_timestamp").cast("timestamp"))
    .dropDuplicates(["vehicle_prefix", "timestamp_api"])
    .filter(col("latitude").isNotNull())
    .filter(col("longitude").isNotNull())
    .filter(col("line_code").isNotNull())
)

df_silver = (
    df_silver
    .withColumn("event_date", to_date("timestamp_api"))
    .withColumn("event_hour", hour("timestamp_api"))
)

df_silver.write \
.format("delta") \
.mode("overwrite") \
.partitionBy("event_date") \
.save(silver_path)

df_check = spark.read.format("delta").load(silver_path)

display(df_check)
