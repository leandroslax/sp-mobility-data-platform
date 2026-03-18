# Databricks notebook source
# MAGIC %run ../00_setup/00_adls_gen2_oauth_connection

# COMMAND ----------

storage_account = "stspmobilitydev001"

landing_path = f"abfss://landing@{storage_account}.dfs.core.windows.net/gtfs"
bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/gtfs"

zip_path = f"{landing_path}/cittamobi_gtfs.zip"
local_zip = "/tmp/cittamobi_gtfs.zip"
extract_dir = "/tmp/gtfs"
extracted_path = f"{landing_path}/extracted"

print("Landing path:", landing_path)
print("Bronze path:", bronze_path)
print("ZIP path:", zip_path)
print("Local ZIP:", local_zip)
print("Extract dir:", extract_dir)
print("Extracted path:", extracted_path)

# COMMAND ----------

dbutils.fs.mkdirs(landing_path)

display(dbutils.fs.ls("abfss://landing@stspmobilitydev001.dfs.core.windows.net/"))

# COMMAND ----------

display(dbutils.fs.ls(landing_path))

# COMMAND ----------

dbutils.fs.cp(zip_path, f"file:{local_zip}", True)

print("Arquivo copiado para:", local_zip)

# COMMAND ----------

import os
import zipfile

os.makedirs(extract_dir, exist_ok=True)

with zipfile.ZipFile(local_zip, "r") as zip_ref:
    zip_ref.extractall(extract_dir)

print("Arquivos extraídos:")
print(sorted(os.listdir(extract_dir)))

# COMMAND ----------

dbutils.fs.mkdirs(extracted_path)

print("Diretório criado:", extracted_path)

# COMMAND ----------

for file_name in os.listdir(extract_dir):
    local_file = f"{extract_dir}/{file_name}"
    target_file = f"{extracted_path}/{file_name}"

    dbutils.fs.cp(f"file:{local_file}", target_file, True)

print("Arquivos enviados para landing/extracted")

# COMMAND ----------

display(dbutils.fs.ls(extracted_path))

# COMMAND ----------

routes_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{extracted_path}/routes.txt")
)

display(routes_df)
