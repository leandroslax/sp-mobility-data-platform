# Databricks notebook source

# Databricks
%run ../00_setup/00_config

# COMMAND ----------

print("🚀 Starting Gold Mobility Metrics...")

# COMMAND ----------

storage_account = "stspmobilitydev001"
container = "bronze"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

silver_path = f"{base_path}/gtfs/silver"
gold_path = f"{base_path}/gtfs/gold"

# COMMAND ----------

print("📥 Reading Silver dataset...")

gtfs = spark.read.format("delta").load(f"{silver_path}/gtfs_enriched")

# COMMAND ----------

from pyspark.sql.functions import countDistinct, count

print("📊 Calculating KPIs...")

kpis = gtfs.agg(
    countDistinct("route_id").alias("total_routes"),
    countDistinct("trip_id").alias("total_trips"),
    countDistinct("stop_id").alias("total_stops"),
    count("*").alias("total_events")
)

display(kpis)

# COMMAND ----------

print("📈 Route performance...")

route_performance = (
    gtfs.groupBy("route_id")
    .agg(
        countDistinct("trip_id").alias("trips_per_route"),
        countDistinct("stop_id").alias("stops_per_route"),
        count("*").alias("events_per_route")
    )
)

display(route_performance)

# COMMAND ----------

print("💾 Saving Gold tables...")

kpis.write.mode("overwrite").format("delta").save(f"{gold_path}/kpis")

route_performance.write.mode("overwrite").format("delta").save(f"{gold_path}/route_performance")

# COMMAND ----------

print("🎯 Gold Mobility Metrics completed!")