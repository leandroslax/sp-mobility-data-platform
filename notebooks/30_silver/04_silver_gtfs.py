# Databricks notebook source
# MAGIC %run ../00_setup/00_config

# COMMAND ----------

print("🥈 Starting Silver GTFS processing...")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

from pyspark.sql.functions import col

# Paths
storage_account = "stspmobilitydev001"
container = "bronze"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

bronze_path = f"{base_path}/gtfs/bronze"
silver_path = f"{base_path}/gtfs/silver"

# COMMAND ----------

print("📂 Reading Bronze data...")

df = spark.read.format("delta").load(bronze_path)

# COMMAND ----------

print("🧹 Basic cleaning...")

df_clean = df.dropDuplicates()

# COMMAND ----------

print("🚍 Processing trips...")

df_trips = df_clean.filter(col("source_file").contains("trips.txt"))

df_trips.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/trips")

# COMMAND ----------

print("🛑 Processing stops...")

df_stops = df_clean.filter(col("source_file").contains("stops.txt"))

df_stops.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/stops")

# COMMAND ----------

print("🗺️ Processing routes...")

df_routes = df_clean.filter(col("source_file").contains("routes.txt"))

df_routes.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/routes")

# COMMAND ----------

print("🎯 Silver layer completed!")
