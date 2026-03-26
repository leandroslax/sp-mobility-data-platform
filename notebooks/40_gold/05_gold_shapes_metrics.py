# Databricks notebook source

# ==========================================
# CONFIG
# ==========================================

container = "bronze"
storage_account = "stspmobilitydev001"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

silver_path = f"{base_path}/gtfs/silver"
gold_path   = f"{base_path}/gtfs/gold"

# ==========================================
# IMPORTS
# ==========================================

from pyspark.sql.functions import col, max

print("🚀 Starting GOLD SHAPES METRICS...")

# ==========================================
# READ SILVER
# ==========================================

df = spark.read.format("delta").load(f"{silver_path}/shapes")

# ==========================================
# CALCULAR DISTÂNCIA
# ==========================================

routes = (
    df.groupBy("shape_id")
    .agg(
        max("shape_dist_traveled").alias("total_distance")
    )
)

routes = routes.orderBy(col("total_distance").desc())

# ==========================================
# WRITE GOLD
# ==========================================

routes.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/routes_distance")

print("✅ GOLD METRICS completed!")
