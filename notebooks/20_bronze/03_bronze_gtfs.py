# Databricks notebook source
# ==========================================
# BRONZE - GTFS PROCESSING
# ==========================================

# 🔧 Importa configurações centralizadas
%run ../00_setup/00_config

from pyspark.sql.functions import current_timestamp

print("🚀 Starting BRONZE GTFS processing...")

# ==========================================
# PATHS
# ==========================================

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

# camada delta (gerada no ingestion)
delta_path = f"{base_path}/gtfs/delta"

# camada bronze (destino)
bronze_path = f"{base_path}/gtfs/bronze"

print(f"📥 Reading from: {delta_path}")
print(f"📤 Writing to: {bronze_path}")

# ==========================================
# READ DELTA (INGESTION OUTPUT)
# ==========================================

df = spark.read.format("delta").load(delta_path)

print("✅ Data loaded successfully")

# ==========================================
# TRANSFORMAÇÃO (BRONZE)
# ==========================================

df_bronze = df.withColumn("ingestion_time", current_timestamp())

# ==========================================
# WRITE BRONZE
# ==========================================

df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .save(bronze_path)

print("✅ BRONZE layer completed successfully!")

# ==========================================
# DEBUG / VALIDATION
# ==========================================

print("🔎 Validating written data...")

df_check = spark.read.format("delta").load(bronze_path)

print(f"📊 Total records in bronze: {df_check.count()}")

display(df_check.limit(10))
