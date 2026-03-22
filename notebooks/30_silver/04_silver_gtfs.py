# Databricks notebook source

# COMMAND ----------
%run ../00_setup/00_config

# COMMAND ----------
print("🚀 Starting Silver GTFS processing...")

# COMMAND ----------
from pyspark.sql.functions import col

# Paths
storage_account = account_name
container = container_bronze

base_path = f"abfss://{container}@{account_fqdn}"

bronze_path = f"{base_path}/gtfs/bronze"
silver_path = f"{base_path}/gtfs/silver"

# COMMAND ----------
print("📥 Reading Bronze tables...")

stops = spark.read.format("delta").load(f"{bronze_path}/stops")
routes = spark.read.format("delta").load(f"{bronze_path}/routes")
trips = spark.read.format("delta").load(f"{bronze_path}/trips")
stop_times = spark.read.format("delta").load(f"{bronze_path}/stop_times")

# COMMAND ----------
print("🧹 Cleaning & casting...")

stops = stops \
    .withColumn("stop_lat", col("stop_lat").cast("double")) \
    .withColumn("stop_lon", col("stop_lon").cast("double"))

stop_times = stop_times \
    .withColumn("stop_sequence", col("stop_sequence").cast("int"))

# COMMAND ----------
print("🔗 Optimizing joins...")

# Broadcast small tables (performance boost)
from pyspark.sql.functions import broadcast

routes = broadcast(routes)
stops = broadcast(stops)

# COMMAND ----------
print("⚙️ Building enriched dataset...")

gtfs_enriched = stop_times \
    .join(trips, "trip_id", "left") \
    .join(routes, "route_id", "left") \
    .join(stops, "stop_id", "left")

# COMMAND ----------
print("💾 Writing Silver tables...")

stops.write.mode("overwrite").format("delta").save(f"{silver_path}/stops")
routes.write.mode("overwrite").format("delta").save(f"{silver_path}/routes")
trips.write.mode("overwrite").format("delta").save(f"{silver_path}/trips")
stop_times.write.mode("overwrite").format("delta").save(f"{silver_path}/stop_times")

# COMMAND ----------
print("💾 Writing enriched dataset...")

gtfs_enriched.write \
    .mode("overwrite") \
    .format("delta") \
    .save(f"{silver_path}/gtfs_enriched")

# COMMAND ----------
print("🎯 Silver GTFS completed!")
