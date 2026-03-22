# Databricks notebook source

# Databricks notebook source

# Databricks
%run ../00_setup/00_config

# COMMAND ----------

print("🚀 Starting GTFS ingestion (production mode)...")

# COMMAND ----------

from pyspark.sql.functions import input_file_name

# Paths
storage_account = "stspmobilitydev001"
container = "bronze"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

zip_path = f"{base_path}/gtfs/raw/cittamobi_gtfs.zip"
extract_path = f"{base_path}/gtfs/extracted"
delta_path = f"{base_path}/gtfs/delta"

# COMMAND ----------

print("📥 Reading ZIP file from ADLS...")

# Spark consegue ler zip como binário
df_binary = spark.read.format("binaryFile").load(zip_path)

display(df_binary)

# COMMAND ----------

import zipfile
import io

print("📦 Extracting ZIP in distributed-friendly way...")

files = []

for row in df_binary.collect():
    content = row.content
    z = zipfile.ZipFile(io.BytesIO(content))
    
    for file_name in z.namelist():
        if file_name.endswith(".txt"):
            files.append((file_name, z.read(file_name).decode("utf-8")))

# COMMAND ----------

print("🧱 Creating Spark DataFrame...")

df_files = spark.createDataFrame(files, ["file_name", "content"])

display(df_files)

# COMMAND ----------

print("💾 Saving raw extracted files (Bronze)...")

df_files.write.mode("overwrite").format("delta").save(delta_path)

# COMMAND ----------

print("🎯 GTFS ingestion completed (production-ready)!")