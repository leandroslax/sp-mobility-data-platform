# Databricks notebook source
# MAGIC %run ../00_setup/00_config

# COMMAND ----------

print("🚀 Starting GTFS ingestion (INCREMENTAL MODE)...")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# Paths
storage_account = "stspmobilitydev001"
container = "bronze"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

raw_path = f"{base_path}/gtfs/raw"
delta_base = f"{base_path}/gtfs/delta"
checkpoint_path = f"{base_path}/gtfs/checkpoints"

# COMMAND ----------

print("📂 Reading new files only (Auto Loader)...")

df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("header", True) \
    .load(raw_path)

# COMMAND ----------

df = df.withColumn("ingestion_time", current_timestamp())

# COMMAND ----------

print("💾 Writing incremental data...")

df.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .start(delta_base)

# COMMAND ----------

print("🎯 Streaming ingestion started!")
