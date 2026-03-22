# Databricks notebook source
# MAGIC %run ../00_setup/00_config

# COMMAND ----------

print("🚀 Starting GTFS ingestion (PERFORMANCE MODE)...")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

from pyspark.sql.functions import input_file_name

# Paths
storage_account = "stspmobilitydev001"
container = "bronze"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

adls_extract_path = f"{base_path}/gtfs/extracted"
delta_path = f"{base_path}/gtfs/delta"

# COMMAND ----------

print("📂 Reading GTFS files from ADLS (distributed)...")

df = spark.read \
    .option("header", True) \
    .option("inferSchema", False) \
    .csv(f"{adls_extract_path}/*.txt")

# COMMAND ----------

print("⚙️ Repartitioning for performance...")

df = df.repartition(8)

# COMMAND ----------

print("💾 Writing to Delta (optimized)...")

df.write \
    .format("delta") \
    .mode("append") \
    .save(delta_path)

# COMMAND ----------

print("🎯 GTFS ingestion completed (FAST MODE)!")
