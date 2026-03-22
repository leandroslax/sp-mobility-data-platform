# Databricks notebook source

print("🚀 Starting Z-ORDER...")

storage_account = "stspmobilitydev001"
container = "bronze"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

# COMMAND ----------

spark.sql(f"""
OPTIMIZE delta.`{base_path}/gtfs/silver/trips`
ZORDER BY (route_id)
""")

# COMMAND ----------

print("🎯 Z-ORDER completed!")
