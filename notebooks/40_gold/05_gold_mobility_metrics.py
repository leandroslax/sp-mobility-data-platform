# Databricks notebook source

# COMMAND ----------
%run ../00_setup/00_config

# COMMAND ----------
print("🚀 Starting Gold Mobility Metrics...")

# COMMAND ----------
from pyspark.sql.functions import countDistinct, count

# Paths
storage_account = account_name
container = container_bronze

base_path = f"abfss://{container}@{account_fqdn}"

silver_path = f"{base_path}/gtfs/silver"
gold_path = f"{base_path}/gtfs/gold"

# COMMAND ----------
print("📥 Reading Silver dataset...")

gtfs = spark.read.format("delta").load(f"{silver_path}/gtfs_enriched")

# COMMAND ----------
print("📊 Calculating KPIs...")

kpis = gtfs.agg(
    countDistinct("route_id").alias("total_routes"),
    countDistinct("trip_id").alias("total_trips"),
    countDistinct("stop_id").alias("total_stops"),
    count("*").alias("total_events")
)

# COMMAND ----------
print("📈 Calculating route performance...")

route_performance = gtfs.groupBy("route_id").agg(
    countDistinct("trip_id").alias("trips_per_route"),
    countDistinct("stop_id").alias("stops_per_route"),
    count("*").alias("events_per_route")
)

# COMMAND ----------
print("💾 Writing Gold tables...")

kpis.write.mode("overwrite").format("delta").save(f"{gold_path}/kpis")

route_performance.write \
    .repartition(1) \
    .write \
    .mode("overwrite") \
    .format("delta") \
    .save(f"{gold_path}/route_performance")

# COMMAND ----------
print("🎯 Gold Mobility Metrics completed!")
