# Databricks notebook source
# MAGIC %run ../00_setup/00_config

from pyspark.sql.functions import current_timestamp

print("🚀 Starting BRONZE GTFS processing...")

# DEBUG
print("DEBUG CONFIG:")
print(f"container={container}")
print(f"storage_account={storage_account}")

# Paths
print("📁 Setting paths...")

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
bronze_path = f"{base_path}/gtfs/bronze"

print(f"Base path: {base_path}")
print(f"Bronze path: {bronze_path}")

# Leitura do delta gerado no ingest
delta_path = f"{base_path}/gtfs/delta"

df = spark.read.format("delta").load(delta_path)

# Adiciona metadata
df = df.withColumn("ingestion_time", current_timestamp())

# Escrita bronze
df.write.format("delta").mode("overwrite").save(bronze_path)

print("✅ BRONZE layer completed!")
