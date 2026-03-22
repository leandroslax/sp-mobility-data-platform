# Databricks notebook source

# MAGIC %run /Workspace/Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/notebooks/00_setup/00_config

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

print("🚀 Starting BRONZE GTFS processing...")

# COMMAND ----------

# Paths
print("📂 Setting paths...")

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
adls_extract_path = f"{base_path}/gtfs/extracted"
delta_base_path = f"{base_path}/gtfs/bronze"

# COMMAND ----------

# Read data (distributed)
print("📥 Reading GTFS files (distributed)...")

df = spark.read \
    .option("header", True) \
    .option("inferSchema", False) \
    .csv(f"{adls_extract_path}/*.txt")

print(f"📊 Columns: {len(df.columns)}")

# COMMAND ----------

# Add metadata
print("🧠 Adding metadata...")

df = df.withColumn("ingestion_time", current_timestamp())

# COMMAND ----------

# Write Delta (optimized)
print("💾 Writing to BRONZE (Delta)...")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(delta_base_path)

# COMMAND ----------

print("✅ BRONZE GTFS completed successfully!")
