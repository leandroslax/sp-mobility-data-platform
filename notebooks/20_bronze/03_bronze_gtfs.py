# Databricks notebook source
# MAGIC %run ../00_setup/config

from pyspark.sql.functions import current_timestamp

config = load_config()

spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

print("Starting BRONZE GTFS processing...")

for entity, bronze_target in config["gtfs_bronze_paths"].items():
    source_path = config["gtfs_extract_paths"][entity]

    try:
        df = spark.read.format("delta").load(source_path)
    except Exception as exc:
        print(f"Skipping {entity}: could not read {source_path} ({exc})")
        continue

    row_count = df.count()
    print(f"Loaded {entity}: {row_count} rows")

    (
        df.withColumn("bronze_loaded_at", current_timestamp())
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(bronze_target)
    )

    print(f"Wrote bronze dataset: {entity} -> {bronze_target}")

print("BRONZE GTFS completed.")

