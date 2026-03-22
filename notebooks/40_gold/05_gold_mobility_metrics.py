# Databricks notebook source
# MAGIC %run ../00_setup/00_config

# COMMAND ----------

print("🥇 Starting Gold Mobility Metrics...")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct

# Paths
storage_account = "stspmobilitydev001"
container = "bronze"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

silver_path = f"{base_path}/gtfs/silver"
gold_path = f"{base_path}/gtfs/gold"

# COMMAND ----------

print("📂 Reading Silver data...")

trips = spark.read.format("delta").load(f"{silver_path}/trips")
stops = spark.read.format("delta").load(f"{silver_path}/stops")
routes = spark.read.format("delta").load(f"{silver_path}/routes")

# COMMAND ----------

print("📊 Calculating KPIs...")

kpis = trips.agg(
    countDistinct("route_id").alias("total_routes"),
    countDistinct("trip_id").alias("total_trips")
)

# COMMAND ----------

print("📈 Trips per route...")

trips_per_route = trips.groupBy("route_id").agg(
    countDistinct("trip_id").alias("trips_count")
)

# COMMAND ----------

print("🛑 Stops per route...")

stops_per_route = stops.groupBy("stop_id").agg(
    count("*").alias("stop_usage")
)

# COMMAND ----------

print("💾 Writing Gold tables...")

kpis.write.format("delta").mode("overwrite").save(f"{gold_path}/kpis")

trips_per_route.write.format("delta").mode("overwrite").save(f"{gold_path}/trips_per_route")

stops_per_route.write.format("delta").mode("overwrite").save(f"{gold_path}/stops_per_route")

# COMMAND ----------

print("🎯 Gold layer completed!")
