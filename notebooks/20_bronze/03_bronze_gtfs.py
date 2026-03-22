# Databricks notebook source
# MAGIC %run ../00_setup/00_config

spark.conf.set("spark.sql.shuffle.partitions", "8")

storage_account = "stspmobilitydev001"
container = "bronze"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

delta_base = f"{base_path}/gtfs/delta"
bronze_base = f"{base_path}/gtfs/bronze"

datasets = ["trips", "stops", "routes", "stop_times"]

for ds in datasets:
    df = spark.read.format("delta").load(f"{delta_base}/{ds}")

    df.write \
        .format("delta") \
        .mode("append") \
        .save(f"{bronze_base}/{ds}")
