# Databricks notebook source
# MAGIC %run "../00_setup/config"

from pyspark.sql.functions import col, current_timestamp, explode

config = load_config()
landing_path = config["sptrans_landing_path"]
bronze_path = config["sptrans_bronze_path"]

dbutils.fs.mkdirs(bronze_path)

print(f"Landing path: {landing_path}")
print(f"Bronze path: {bronze_path}")

df_raw = spark.read.json(landing_path)

df_lines = df_raw.select(
    "hr",
    explode("l").alias("line"),
)

df_vehicles = df_lines.select(
    col("hr"),
    col("line.c").alias("line_code"),
    col("line.cl").alias("line_id"),
    col("line.lt0").alias("line_name"),
    explode(col("line.vs")).alias("vehicle"),
)

df_bronze = df_vehicles.select(
    "hr",
    "line_code",
    "line_id",
    "line_name",
    col("vehicle.p").alias("vehicle_prefix"),
    col("vehicle.a").alias("accessible"),
    col("vehicle.ta").alias("timestamp_api"),
    col("vehicle.py").alias("latitude"),
    col("vehicle.px").alias("longitude"),
).withColumn("ingestion_timestamp", current_timestamp())

(
    df_bronze.write.format("delta")
    .mode("append")
    .save(bronze_path)
)

print(f"Bronze SPTrans rows appended to {bronze_path}")
