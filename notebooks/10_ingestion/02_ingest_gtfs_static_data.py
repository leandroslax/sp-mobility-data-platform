# Databricks notebook source
# MAGIC %run ../00_setup/00_config

# COMMAND ----------

print("🚀 Starting GTFS ingestion (FINAL OPTIMIZED)...")

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
adls_extract_path = f"{base_path}/gtfs/extracted"

# COMMAND ----------

print("📥 Copying ZIP...")

dbutils.fs.cp(zip_path, f"file:{local_zip}", True)

# COMMAND ----------

print("📦 Extracting ZIP...")

os.makedirs(extract_path, exist_ok=True)

with zipfile.ZipFile(local_zip, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

# COMMAND ----------

print("⬆️ Moving files to ADLS...")

files = os.listdir(extract_path)

for f in files:
    dbutils.fs.cp(f"file:{extract_path}/{f}", f"{adls_extract_path}/{f}", True)

# COMMAND ----------

print("📥 Reading from ADLS (distributed)...")

df = spark.read.option("header", True).csv(f"{adls_extract_path}/*.txt")

display(df)

# COMMAND ----------

delta_path = f"{base_path}/gtfs/delta"

df.write \
  .format("delta") \
  .mode("overwrite") \
  .save(delta_path)

# COMMAND ----------

print("✅ GTFS ingestion completed FAST!")
