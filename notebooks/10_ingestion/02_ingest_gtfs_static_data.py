# Databricks notebook source
# MAGIC %run ../00_setup/00_config

# COMMAND ----------

print("🚀 Starting GTFS ingestion (ULTRA PERFORMANCE MODE)...")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# Paths
storage_account = "stspmobilitydev001"
container = "bronze"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

adls_extract_path = f"{base_path}/gtfs/extracted"
delta_base_path = f"{base_path}/gtfs/delta"

# COMMAND ----------

def process_file(file_name):
    print(f"📂 Processing {file_name}...")

    df = spark.read \
        .option("header", True) \
        .option("inferSchema", False) \
        .csv(f"{adls_extract_path}/{file_name}")

    df = df.repartition(4)

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{delta_base_path}/{file_name.replace('.txt','')}")

# COMMAND ----------

files = [
    "trips.txt",
    "stops.txt",
    "routes.txt",
    "stop_times.txt"
]

for f in files:
    process_file(f)

# COMMAND ----------

print("🎯 GTFS ingestion completed (FAST + CORRECT)!")
