%run ../00_setup/00_config
# Databricks notebook source

print("⚡ Starting Delta optimization...")

# COMMAND ----------

storage_account = "stspmobilitydev001"
container = "bronze"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

tables = [
    "gtfs/bronze/trips",
    "gtfs/bronze/stops",
    "gtfs/silver/trips",
    "gtfs/silver/stops",
    "gtfs/gold/kpis"
]

# COMMAND ----------

for table in tables:
    path = f"{base_path}/{table}"
    
    print(f"⚙️ Optimizing {table}...")
    
    spark.sql(f"OPTIMIZE delta.`{path}`")

# COMMAND ----------

print("🎯 Optimization completed!")