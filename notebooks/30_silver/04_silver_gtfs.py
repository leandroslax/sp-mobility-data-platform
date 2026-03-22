# Databricks notebook source
# MAGIC %run ../00_setup/00_config

spark.conf.set("spark.sql.shuffle.partitions", "8")

storage_account = "stspmobilitydev001"
container = "bronze"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

bronze_base = f"{base_path}/gtfs/bronze"
silver_base = f"{base_path}/gtfs/silver"

datasets = ["trips", "stops", "routes", "stop_times"]

for ds in datasets:
    df = spark.read.format("delta").load(f"{bronze_base}/{ds}")

    df = df.dropDuplicates()

    df.write \
        .format("delta") \
        .mode("append") \
        .save(f"{silver_base}/{ds}")
