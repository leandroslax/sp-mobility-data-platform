# Databricks
%run ../00_setup/00_config

# COMMAND ----------

print("🚀 Starting Silver GTFS processing...")

# COMMAND ----------

storage_account = "stspmobilitydev001"
container = "bronze"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

bronze_path = f"{base_path}/gtfs/bronze"
silver_path = f"{base_path}/gtfs/silver"

# COMMAND ----------

print("📥 Reading Bronze tables...")

stops = spark.read.format("delta").load(f"{bronze_path}/stops")
routes = spark.read.format("delta").load(f"{bronze_path}/routes")
trips = spark.read.format("delta").load(f"{bronze_path}/trips")
stop_times = spark.read.format("delta").load(f"{bronze_path}/stop_times")

# COMMAND ----------

from pyspark.sql.functions import col

print("🧹 Cleaning & casting data...")

stops_clean = (
    stops
    .withColumn("stop_lat", col("stop_lat").cast("double"))
    .withColumn("stop_lon", col("stop_lon").cast("double"))
)

routes_clean = routes

trips_clean = trips

stop_times_clean = (
    stop_times
    .withColumn("stop_sequence", col("stop_sequence").cast("int"))
)

# COMMAND ----------

print("🔗 Creating enriched dataset...")

gtfs_enriched = (
    stop_times_clean
    .join(trips_clean, "trip_id", "left")
    .join(routes_clean, "route_id", "left")
    .join(stops_clean, "stop_id", "left")
)

display(gtfs_enriched)

# COMMAND ----------

print("💾 Saving Silver tables...")

stops_clean.write.mode("overwrite").format("delta").save(f"{silver_path}/stops")
routes_clean.write.mode("overwrite").format("delta").save(f"{silver_path}/routes")
trips_clean.write.mode("overwrite").format("delta").save(f"{silver_path}/trips")
stop_times_clean.write.mode("overwrite").format("delta").save(f"{silver_path}/stop_times")

gtfs_enriched.write.mode("overwrite").format("delta").save(f"{silver_path}/gtfs_enriched")

# COMMAND ----------

print("🎯 Silver GTFS completed!")