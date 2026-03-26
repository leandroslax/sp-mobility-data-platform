# Databricks notebook source
# MAGIC %run "../00_setup/config"

from pyspark.sql.functions import collect_list, col, sort_array, struct

config = load_config()

print("Starting GEO layers processing...")

df = spark.read.format("delta").load(config["gtfs_silver_shapes_path"])

points = df.select(
    col("shape_id"),
    struct(
        col("shape_pt_sequence"),
        col("shape_pt_lat"),
        col("shape_pt_lon"),
    ).alias("point"),
)

routes_geo = points.groupBy("shape_id").agg(
    sort_array(collect_list("point")).alias("points")
)

(
    routes_geo.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{config['gold_root']}/gtfs/geo_routes")
)

print("GEO layers completed.")
