# Databricks notebook source
# MAGIC %run ../00_setup/config

from pyspark.sql.functions import col, hour, to_date

config = load_config()
bronze_path = config["sptrans_bronze_path"]
silver_path = config["sptrans_silver_path"]

df_bronze = spark.read.format("delta").load(bronze_path)

df_silver = (
    df_bronze.withColumn("timestamp_api", col("timestamp_api").cast("timestamp"))
    .withColumn("ingestion_timestamp", col("ingestion_timestamp").cast("timestamp"))
    .dropDuplicates(["vehicle_prefix", "timestamp_api"])
    .filter(col("latitude").isNotNull())
    .filter(col("longitude").isNotNull())
    .filter(col("line_code").isNotNull())
    .withColumn("event_date", to_date("timestamp_api"))
    .withColumn("event_hour", hour("timestamp_api"))
)

(
    df_silver.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date")
    .save(silver_path)
)

print(f"Silver SPTrans dataset refreshed at {silver_path}")

