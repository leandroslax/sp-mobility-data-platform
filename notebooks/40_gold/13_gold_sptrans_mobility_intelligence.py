# Databricks notebook source
# MAGIC %run ../00_setup/config

from pyspark.sql import functions as F

config = load_config()

routes_df = (
    spark.read.format("delta")
    .load(config["gtfs_bronze_paths"]["routes"])
    .dropDuplicates(["route_id"])
)

(
    routes_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(config["gtfs_routes_silver_path"])
)

vehicle_positions_df = spark.read.format("delta").load(config["sptrans_silver_path"])
routes_df = spark.read.format("delta").load(config["gtfs_routes_silver_path"])

mobility_intelligence_df = vehicle_positions_df.alias("vp").join(
    routes_df.alias("rt"),
    F.col("vp.line_code").cast("string") == F.col("rt.route_short_name").cast("string"),
    "left",
).select(
    F.col("vp.line_code"),
    F.col("vp.line_id"),
    F.col("vp.line_name"),
    F.col("vp.vehicle_prefix"),
    F.col("vp.accessible"),
    F.col("vp.timestamp_api"),
    F.col("vp.latitude"),
    F.col("vp.longitude"),
    F.col("vp.event_date"),
    F.col("vp.event_hour"),
    F.col("rt.route_id"),
    F.col("rt.route_short_name"),
    F.col("rt.route_long_name"),
    F.col("rt.route_type"),
)

gold_mobility_df = mobility_intelligence_df.groupBy(
    "event_date",
    "event_hour",
    "line_code",
    "line_id",
    "line_name",
    "route_id",
    "route_short_name",
    "route_long_name",
    "route_type",
).agg(F.countDistinct("vehicle_prefix").alias("active_vehicles"))

(
    gold_mobility_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(config["mobility_intelligence_path"])
)

print(f"Mobility intelligence refreshed at {config['mobility_intelligence_path']}")

