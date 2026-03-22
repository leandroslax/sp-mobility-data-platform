# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../00_setup/00_adls_gen2_oauth_connection
# COMMAND ----------
from pyspark.sql.functions import *

silver_path = "abfss://silver@stspmobilitydev001.dfs.core.windows.net/sptrans/vehicle_positions"
gold_path = "abfss://gold@stspmobilitydev001.dfs.core.windows.net/sptrans/vehicle_positions"

df_silver = spark.read.format("delta").load(silver_path)

display(df_silver)

df_gold_line_activity = (
    df_silver
    .groupBy("line_code", "line_name", "event_date", "event_hour")
    .agg(
        countDistinct("vehicle_prefix").alias("active_vehicles")
    )
)

df_gold_geo = (
    df_silver
    .groupBy("line_code", "line_name")
    .agg(
        avg("latitude").alias("avg_latitude"),
        avg("longitude").alias("avg_longitude"),
        countDistinct("vehicle_prefix").alias("fleet_size")
    )
)

df_gold_line_activity.write \
.format("delta") \
.mode("overwrite") \
.save(f"{gold_path}/line_activity")

df_gold_geo.write \
.format("delta") \
.mode("overwrite") \
.save(f"{gold_path}/line_geo")

df_check = spark.read.format("delta").load(f"{gold_path}/line_activity")

display(df_check)
