# Databricks notebook source
# MAGIC %run ../00_setup/00_config

# COMMAND ----------

print("🥉 Starting Bronze layer (dataset-oriented)...")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

storage_account = "stspmobilitydev001"
container = "bronze"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

delta_base = f"{base_path}/gtfs/delta"
bronze_base = f"{base_path}/gtfs/bronze"

# COMMAND ----------

datasets = ["trips", "stops", "routes", "stop_times"]

for ds in datasets:
    print(f"📂 Processing {ds}...")

    df = spark.read.format("delta").load(f"{delta_base}/{ds}")

    df = df.repartition(4)

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{bronze_base}/{ds}")

# COMMAND ----------

print("🎯 Bronze completed!")
