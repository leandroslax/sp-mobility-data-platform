# Databricks notebook source
# MAGIC %run ../00_setup/config

from pyspark.sql import functions as F

config = load_config()

df = spark.read.format("delta").load(config["sptrans_silver_path"])

city_heatmap_df = (
    df.filter(F.col("event_date").isNotNull())
    .filter(F.col("event_hour").isNotNull())
    .filter(F.col("vehicle_prefix").isNotNull())
    .filter(F.col("latitude").isNotNull())
    .filter(F.col("longitude").isNotNull())
    .withColumn("lat_grid", F.round(F.col("latitude"), 2))
    .withColumn("lon_grid", F.round(F.col("longitude"), 2))
    .groupBy("event_date", "event_hour", "lat_grid", "lon_grid")
    .agg(
        F.countDistinct("vehicle_prefix").alias("active_vehicles"),
        F.count("*").alias("total_positions"),
        F.countDistinct("line_code").alias("active_lines"),
        F.sum(F.when(F.col("accessible") == True, 1).otherwise(0)).alias(
            "accessible_positions"
        ),
    )
    .withColumn(
        "accessibility_pct",
        F.when(
            F.col("total_positions") > 0,
            F.round(F.col("accessible_positions") / F.col("total_positions"), 4),
        ).otherwise(F.lit(0.0)),
    )
)

(
    city_heatmap_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date")
    .save(config["city_heatmap_path"])
)

print(f"City heatmap refreshed at {config['city_heatmap_path']}")

