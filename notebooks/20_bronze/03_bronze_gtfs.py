# Databricks notebook source

# Databricks notebook source

# Databricks
%run ../00_setup/00_config

# COMMAND ----------

print("🚀 Starting Bronze GTFS processing...")

# COMMAND ----------

storage_account = "stspmobilitydev001"
container = "bronze"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

raw_path = f"{base_path}/gtfs/raw"
bronze_path = f"{base_path}/gtfs/bronze"

# COMMAND ----------

print("📂 Listing GTFS raw files...")
files = dbutils.fs.ls(f"{raw_path}/")

display(files)

# COMMAND ----------

import zipfile
import io

print("📦 Reading ZIP...")

zip_file = f"{raw_path}/cittamobi_gtfs.zip"

binary_df = spark.read.format("binaryFile").load(zip_file)

files_data = {}

for row in binary_df.collect():
    z = zipfile.ZipFile(io.BytesIO(row.content))
    
    for file_name in z.namelist():
        if file_name.endswith(".txt"):
            content = z.read(file_name).decode("utf-8")
            files_data[file_name] = content

# COMMAND ----------

from pyspark.sql import Row

def save_gtfs_table(file_name, content):
    print(f"💾 Processing {file_name}...")

    lines = content.split("\n")
    header = lines[0].split(",")
    
    rows = []
    for line in lines[1:]:
        if line.strip():
            values = line.split(",")
            rows.append(Row(**dict(zip(header, values))))
    
    df = spark.createDataFrame(rows)
    
    table_name = file_name.replace(".txt", "")
    
    df.write.mode("overwrite").format("delta").save(f"{bronze_path}/{table_name}")
    
    print(f"✅ Saved: {table_name}")

# COMMAND ----------

for file_name, content in files_data.items():
    save_gtfs_table(file_name, content)

# COMMAND ----------

print("🎯 Bronze GTFS completed!")