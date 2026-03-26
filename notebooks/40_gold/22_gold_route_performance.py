# Databricks notebook source
# MAGIC %run /Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/notebooks/00_setup/config

# COMMAND ----------

from pyspark.sql import functions as F

config = load_config()

df = spark.read.format("delta").load(config["sptrans_silver_path"])

route_performance_df = (
    df.filter(F.col("line_code").isNotNull())
    .filter(F.col("vehicle_prefix").isNotNull())
    .filter(F.col("event_date").isNotNull())
    .filter(F.col("event_hour").isNotNull())
    .groupBy("event_date", "event_hour", "line_code")
    .agg(
        F.countDistinct("vehicle_prefix").alias("active_vehicles"),
        F.count("*").alias("total_positions"),
        F.avg("latitude").alias("avg_latitude"),
        F.avg("longitude").alias("avg_longitude"),
    )
)

(
    route_performance_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date")
    .save(config["route_performance_path"])
)

print(f"Route performance refreshed at {config['route_performance_path']}")
