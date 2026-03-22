# Databricks notebook source
# MAGIC %run ../00_setup/00_config

# COMMAND ----------

print("🚀 Starting GTFS ingestion (FULL DISTRIBUTED MODE)...")

# COMMAND ----------

# PERFORMANCE CONFIG
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# COMMAND ----------

# Paths
storage_account = "stspmobilitydev001"
container = "bronze"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
adls_extract_path = f"{base_path}/gtfs/extracted"
delta_base_path = f"{base_path}/gtfs/delta"

# COMMAND ----------

print("📂 Reading ALL GTFS files in parallel...")

df = spark.read \
    .option("header", True) \
    .option("inferSchema", False) \
    .csv(f"{adls_extract_path}/*.txt")

# COMMAND ----------

print("⚡ Repartitioning for max performance...")
df = df.repartition(8)

# COMMAND ----------

print("💾 Writing to Delta (optimized)...")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(delta_base_path)

# COMMAND ----------

print("🎯 GTFS ingestion completed (FULL DISTRIBUTED 🚀)!")
