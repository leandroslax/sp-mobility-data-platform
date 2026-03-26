# Databricks notebook source
# MAGIC %run ../00_setup/config

from pyspark.sql.functions import col, max

config = load_config()

print("Starting GOLD shapes metrics...")

df = spark.read.format("delta").load(config["gtfs_silver_shapes_path"])

routes = (
    df.groupBy("shape_id")
    .agg(max("shape_dist_traveled").alias("total_distance"))
    .orderBy(col("total_distance").desc())
)

(
    routes.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{config['gold_root']}/gtfs/routes_distance")
)

print("GOLD shapes metrics completed.")

