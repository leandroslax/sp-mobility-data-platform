# Databricks notebook source

# COMMAND ----------
print("🚀 Starting GTFS ingestion...")

# COMMAND ----------
zip_source_path = "abfss://bronze@stspmobilitydev001.dfs.core.windows.net/gtfs/raw/cittamobi_gtfs.zip"
zip_local_path = "/tmp/cittamobi_gtfs.zip"
extract_path = "/tmp/gtfs"

# COMMAND ----------
print("📥 Copying file from ADLS...")
dbutils.fs.cp(zip_source_path, f"file:{zip_local_path}", True)

# COMMAND ----------
import zipfile
import os

print("📦 Extracting zip...")

os.makedirs(extract_path, exist_ok=True)

with zipfile.ZipFile(zip_local_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

# COMMAND ----------
print("📂 Files extracted:")
for f in os.listdir(extract_path):
    print(f)

# COMMAND ----------
print("🎯 GTFS ingestion completed!")
