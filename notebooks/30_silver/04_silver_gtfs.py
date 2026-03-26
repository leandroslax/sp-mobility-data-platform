# Databricks notebook source

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

from pyspark.sql.functions import col

print("🚀 Starting SILVER GTFS processing...")

# COMMAND ----------

# Paths
base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

bronze_path = f"{base_path}/gtfs/bronze"
silver_base_path = f"{base_path}/gtfs/silver"

# COMMAND ----------

print("📥 Reading BRONZE data...")

df = spark.read.format("delta").load(bronze_path)

print(f"📊 Total columns: {len(df.columns)}")

# COMMAND ----------

# 🔹 STOPS
print("🧹 Processing STOPS...")

stops = df.select(
    "stop_id",
    "stop_name",
    "stop_lat",
    "stop_lon"
).dropDuplicates()

stops.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_base_path}/stops")

# COMMAND ----------

# 🔹 ROUTES
print("🧹 Processing ROUTES...")

routes = df.select(
    "route_id",
    "route_short_name",
    "route_long_name",
    "route_type"
).dropDuplicates()

routes.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_base_path}/routes")

# COMMAND ----------

# 🔹 TRIPS
print("🧹 Processing TRIPS...")

trips = df.select(
    "trip_id",
    "route_id",
    "service_id"
).dropDuplicates()

trips.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_base_path}/trips")

# COMMAND ----------

# 🔹 STOP TIMES
print("🧹 Processing STOP_TIMES...")

stop_times = df.select(
    "trip_id",
    "arrival_time",
    "departure_time",
    "stop_id",
    "stop_sequence"
)

stop_times.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_base_path}/stop_times")

# COMMAND ----------

print("✅ SILVER GTFS completed successfully!")