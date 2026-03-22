# Databricks notebook source
# MAGIC %run ../00_setup/00_config

spark.conf.set("spark.sql.shuffle.partitions", "8")

from pyspark.sql.functions import countDistinct

storage_account = "stspmobilitydev001"
container = "bronze"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

silver_base = f"{base_path}/gtfs/silver"
gold_base = f"{base_path}/gtfs/gold"

trips = spark.read.format("delta").load(f"{silver_base}/trips")

kpis = trips.agg(
    countDistinct("route_id").alias("total_routes"),
    countDistinct("trip_id").alias("total_trips")
)

kpis.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_base}/kpis")