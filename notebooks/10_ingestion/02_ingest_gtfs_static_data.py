# Databricks notebook source
# MAGIC %run /Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/notebooks/00_setup/config

import io
import zipfile

import requests
from pyspark.sql.functions import current_timestamp, lit

config = load_config()

GTFS_DOWNLOAD_URL = "https://transitfeeds.com/p/sptrans/911/latest/download"

print("Starting GTFS ingestion...")
print(f"Environment: {config['env']}")
print(f"Extract target: {config['gtfs_extract_path']}")

response = requests.get(GTFS_DOWNLOAD_URL, timeout=(30, 180))
response.raise_for_status()

archive = zipfile.ZipFile(io.BytesIO(response.content))

processed_entities = []

for file_name in sorted(archive.namelist()):
    if not file_name.lower().endswith((".txt", ".csv")):
        continue

    entity = file_name.rsplit("/", 1)[-1].rsplit(".", 1)[0].lower()
    target_path = config["gtfs_extract_paths"].get(entity)

    if not target_path:
        print(f"Skipping unsupported GTFS entity: {file_name}")
        continue

    print(f"Processing GTFS entity: {entity}")

    csv_content = archive.read(file_name).decode("utf-8").splitlines()
    df = spark.read.option("header", True).csv(
        spark.sparkContext.parallelize(csv_content)
    )

    (
        df.withColumn("source_file", lit(file_name))
        .withColumn("ingestion_timestamp", current_timestamp())
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target_path)
    )

    processed_entities.append((entity, target_path))

print("GTFS ingestion completed.")

for entity, target_path in processed_entities:
    print(f"OK: {entity} -> {target_path}")
