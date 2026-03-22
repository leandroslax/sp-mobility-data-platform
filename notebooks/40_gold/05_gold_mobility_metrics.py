# Databricks notebook source
# MAGIC %run ../00_setup/00_config

# COMMAND ----------

print("🥇 Starting Gold layer (analytics)...")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

from pyspark.sql.functions import countDistinct

storage_account = "stspmobilitydev001"
container = "bronze"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

silver_base = f"{base_path}/gtfs/silver"
gold_base = f"{base_path}/gtfs/gold"

# COMMAND ----------

trips = spark.read.format("delta").load(f"{silver_base}/trips")
routes = spark.read.format("delta").load(f"{silver_base}/routes")

# COMMAND ----------

print("📊 Joining trips + routes...")

df_join = trips.join(routes, "route_id", "left")

# COMMAND ----------

print("📈 KPIs...")

kpis = df_join.agg(
    countDistinct("route_id").alias("total_routes"),
    countDistinct("trip_id").alias("total_trips")
)

# COMMAND ----------

print("💾 Writing Gold...")

kpis.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_base}/kpis")

# COMMAND ----------

print("🎯 Gold completed!")
