# Databricks notebook source
# MAGIC %run /Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/notebooks/00_setup/config

from pyspark.sql import functions as F

config = load_config()

df = spark.read.format("delta").load(config["sptrans_silver_path"])

city_activity_df = (
    df.filter(F.col("line_code").isNotNull())
    .filter(F.col("latitude").isNotNull())
    .filter(F.col("longitude").isNotNull())
    .groupBy("event_date", "event_hour")
    .agg(
        F.countDistinct("line_code").alias("active_lines"),
        F.countDistinct("vehicle_prefix").alias("active_vehicles"),
        F.count("*").alias("total_positions"),
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
    city_activity_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date")
    .save(config["city_activity_path"])
)

print(f"City activity refreshed at {config['city_activity_path']}")
