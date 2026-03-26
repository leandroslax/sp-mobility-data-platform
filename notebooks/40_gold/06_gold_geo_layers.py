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

from pyspark.sql.functions import col, collect_list, struct, sort_array

print("🗺️ Starting GEO LAYERS processing...")

# ==========================================
# READ SILVER
# ==========================================

df = spark.read.format("delta").load(f"{silver_path}/shapes")

# ==========================================
# ORGANIZAR PONTOS
# ==========================================

points = (
    df.select(
        col("shape_id"),
        struct(
            col("shape_pt_sequence"),
            col("shape_pt_lat"),
            col("shape_pt_lon")
        ).alias("point")
    )
)

# ==========================================
# AGRUPAR EM LINHAS
# ==========================================

routes_geo = (
    points.groupBy("shape_id")
    .agg(
        sort_array(collect_list("point")).alias("points")
    )
)

# ==========================================
# WRITE GOLD
# ==========================================

routes_geo.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/geo_routes")

print("✅ GEO LAYERS completed!")
