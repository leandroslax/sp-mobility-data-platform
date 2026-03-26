# Databricks notebook source
# MAGIC %run /Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/notebooks/00_setup/config

from pyspark.sql.functions import avg, countDistinct

config = load_config()
silver_path = config["sptrans_silver_path"]
gold_path = config["sptrans_gold_path"]

df_silver = spark.read.format("delta").load(silver_path)

df_gold_line_activity = df_silver.groupBy(
    "line_code",
    "line_name",
    "event_date",
    "event_hour",
).agg(countDistinct("vehicle_prefix").alias("active_vehicles"))

df_gold_geo = df_silver.groupBy("line_code", "line_name").agg(
    avg("latitude").alias("avg_latitude"),
    avg("longitude").alias("avg_longitude"),
    countDistinct("vehicle_prefix").alias("fleet_size"),
)

(
    df_gold_line_activity.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{gold_path}/line_activity")
)

(
    df_gold_geo.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{gold_path}/line_geo")
)

print(f"Gold SPTrans analytics refreshed at {gold_path}")
