# Databricks notebook source

# COMMAND ----------
%run ../00_setup/00_config

# COMMAND ----------
print("🚀 Starting GTFS ingestion (ULTRA PERFORMANCE mode)...")

# COMMAND ----------
import zipfile
import io

# COMMAND ----------
# Paths
storage_account = account_name
container = container_bronze

base_path = f"abfss://{container}@{account_fqdn}"

zip_path = f"{base_path}/gtfs/raw/cittamobi_gtfs.zip"
extract_path = f"{base_path}/gtfs/extracted"
delta_path = f"{base_path}/gtfs/delta"

# COMMAND ----------
print("📥 Reading ZIP as binary (no collect)...")

df_binary = spark.read.format("binaryFile").load(zip_path)

# COMMAND ----------
print("📦 Extracting ZIP and writing directly to ADLS...")

def extract_and_save(row):
    import zipfile
    import io

    z = zipfile.ZipFile(io.BytesIO(row.content))

    for file_name in z.namelist():
        if file_name.endswith(".txt"):
            file_content = z.read(file_name)

            output_path = f"{extract_path}/{file_name}"

            dbutils.fs.put(output_path, file_content.decode("utf-8"), True)

df_binary.foreach(extract_and_save)

# COMMAND ----------
print("📥 Reading extracted files with Spark (distributed)...")

df = spark.read.option("header", True).csv(f"{extract_path}/*.txt")

display(df)

# COMMAND ----------
print("💾 Saving as Delta...")

df.write.mode("overwrite").format("delta").save(delta_path)

# COMMAND ----------
print("✅ GTFS ingestion completed (ULTRA FAST)!")
