# Databricks notebook source

# ==========================================
# CONFIG INLINE (JOB SAFE)
# ==========================================

container = "bronze"
storage_account = "stspmobilitydev001"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

gtfs_base_path = f"{base_path}/gtfs"

gtfs_extract_path = f"{gtfs_base_path}/extracted"
gtfs_bronze_path  = f"{gtfs_base_path}/bronze"

# ==========================================
# IMPORTS
# ==========================================

import requests
import zipfile
import io

print("🚀 Starting GTFS ingestion...")

# ==========================================
# DOWNLOAD GTFS
# ==========================================

url = "https://transitfeeds.com/p/sptrans/911/latest/download"

response = requests.get(url)

z = zipfile.ZipFile(io.BytesIO(response.content))

print("📦 Extracting files...")

# ==========================================
# PROCESS FILES
# ==========================================

for file in z.namelist():
    print(f"Processing: {file}")

    df = spark.read.option("header", True).csv(
        spark.sparkContext.parallelize(
            z.read(file).decode("utf-8").splitlines()
        )
    )

    df.write.format("delta") \
        .mode("overwrite") \
        .save(f"{gtfs_base_path}/delta")

print("✅ Ingestion completed!")
