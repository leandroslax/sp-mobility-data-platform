# Databricks notebook source

# COMMAND ----------
%run ../00_setup/00_config

# COMMAND ----------

print("🚀 Starting GTFS ingestion (production mode)...")

# COMMAND ----------

from pyspark.sql.functions import input_file_name
import zipfile
import io

# COMMAND ----------

# Paths
storage_account = account_name
container = container_bronze

base_path = f"abfss://{container}@{account_fqdn}"

zip_path = f"{base_path}/gtfs/raw/cittamobi_gtfs.zip"
delta_path = f"{base_path}/gtfs/delta"

# COMMAND ----------

print("📥 Reading ZIP file from ADLS...")

df_binary = spark.read.format("binaryFile").load(zip_path)

print(f"Arquivos encontrados: {df_binary.count()}")

# COMMAND ----------

print("📦 Extracting ZIP...")

files = []

for row in df_binary.collect():
    content = row.content
    z = zipfile.ZipFile(io.BytesIO(content))
    
    for file_name in z.namelist():
        if file_name.endswith(".txt"):
            files.append((file_name, z.read(file_name).decode("utf-8")))

print(f"Total de arquivos extraídos: {len(files)}")

# COMMAND ----------

print("🧱 Creating DataFrame...")

df_files = spark.createDataFrame(files, ["file_name", "content"])

display(df_files)

# COMMAND ----------

print("💾 Saving Bronze Delta...")

df_files.write.mode("overwrite").format("delta").save(delta_path)

# COMMAND ----------

print("✅ GTFS ingestion completed!")
