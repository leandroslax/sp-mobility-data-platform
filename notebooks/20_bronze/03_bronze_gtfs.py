# Databricks notebook source
# MAGIC %run ../00_setup/00_config

from pyspark.sql.functions import current_timestamp

print("🚀 Starting BRONZE GTFS processing...")

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
bronze_path = f"{base_path}/gtfs/bronze"
delta_path = f"{base_path}/gtfs/delta"

df = spark.read.format("delta").load(delta_path)

df = df.withColumn("ingestion_time", current_timestamp())

df.write.format("delta").mode("overwrite").save(bronze_path)

print("✅ BRONZE layer completed!")