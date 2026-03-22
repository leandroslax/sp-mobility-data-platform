# Databricks notebook source

# COMMAND ----------
%run ../00_setup/00_config

# COMMAND ----------
print("🚀 Starting GTFS ingestion (PERFORMANCE MODE)...")

# COMMAND ----------
import zipfile
import os

# COMMAND ----------
# Paths
storage_account = account_name
container = container_bronze

base_path = f"abfss://{container}@{account_fqdn}"

zip_path = f"{base_path}/gtfs/raw/cittamobi_gtfs.zip"
local_zip = "/tmp/gtfs.zip"
extract_path = "/tmp/gtfs"

# COMMAND ----------
print("📥 Copying ZIP to local...")

dbutils.fs.cp(zip_path, f"file:{local_zip}", True)

# COMMAND ----------
print("📦 Extracting ZIP...")

os.makedirs(extract_path, exist_ok=True)

with zipfile.ZipFile(local_zip, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

# COMMAND ----------
print("📥 Loading with Spark (parallelized)...")

df = spark.read.option("header", True).csv(f"file:{extract_path}/*.txt")

# 🔥 AQUI ESTÁ O BOOST
df = df.repartition(8)

display(df)

# COMMAND ----------
delta_path = f"{base_path}/gtfs/delta"

print("💾 Writing Delta (optimized)...")

df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .save(delta_path)

# COMMAND ----------
print("✅ GTFS ingestion completed FAST!")
