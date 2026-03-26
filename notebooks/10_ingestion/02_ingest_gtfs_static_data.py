# Databricks notebook source

def _get_widget(name, default_value):
    try:
        dbutils.widgets.text(name, default_value)
    except Exception:
        pass
    try:
        value = dbutils.widgets.get(name)
        return value or default_value
    except Exception:
        return default_value

import io
import zipfile
from pathlib import Path

import requests
from pyspark.sql.functions import current_timestamp, lit


def load_config():
    env = _get_widget("env", "dev")
    storage_account = _get_widget("storage_account", "stspmobilitydev001")
    landing_root = f"abfss://landing@{storage_account}.dfs.core.windows.net"
    gtfs_entities = {
        "agency": "gtfs_agency",
        "calendar": "gtfs_calendar",
        "calendar_dates": "gtfs_calendar_dates",
        "feed_info": "gtfs_feed_info",
        "routes": "gtfs_routes",
        "shapes": "gtfs_shapes",
        "stop_times": "gtfs_stop_times",
        "stops": "gtfs_stops",
        "trips": "gtfs_trips",
    }
    return {
        "env": env,
        "gtfs_extract_path": f"{landing_root}/gtfs/extracted",
        "gtfs_extract_paths": {
            entity: f"{landing_root}/gtfs/extracted/{entity}"
            for entity in gtfs_entities
        },
    }

config = load_config()

GTFS_DOWNLOAD_URL = "https://transitfeeds.com/p/sptrans/911/latest/download"
LOCAL_GTFS_CANDIDATES = [
    "/Workspace/Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/data/raw/gtfs/cittamobi_gtfs.zip",
    "/Workspace/Repos/leandroslax/sp-mobility-data-platform/data/raw/gtfs/cittamobi_gtfs.zip",
]


def load_gtfs_archive():
    try:
        response = requests.get(
            GTFS_DOWNLOAD_URL,
            timeout=(30, 180),
            headers={"User-Agent": "Mozilla/5.0 Databricks GTFS Ingestion"},
        )
        response.raise_for_status()
        print(f"Downloaded GTFS from remote source: {GTFS_DOWNLOAD_URL}")
        return zipfile.ZipFile(io.BytesIO(response.content))
    except requests.HTTPError as exc:
        print(f"Remote GTFS download failed: {exc}")
    except Exception as exc:
        print(f"Unexpected remote GTFS error: {exc}")

    for candidate in LOCAL_GTFS_CANDIDATES:
        path = Path(candidate)
        if path.exists():
            print(f"Using local GTFS fallback: {candidate}")
            return zipfile.ZipFile(path)

    raise Exception(
        "Unable to load GTFS archive from remote source or local fallback paths."
    )

print("Starting GTFS ingestion...")
print(f"Environment: {config['env']}")
print(f"Extract target: {config['gtfs_extract_path']}")

archive = load_gtfs_archive()
archive_entries = sorted(archive.namelist())

print(f"Archive entries found: {len(archive_entries)}")
for entry in archive_entries:
    print(f" - {entry}")

processed_entities = []

for file_name in archive_entries:
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

if not processed_entities:
    raise Exception(
        "No supported GTFS entities were processed from the downloaded archive."
    )

print("GTFS ingestion completed.")

for entity, target_path in processed_entities:
    print(f"OK: {entity} -> {target_path}")
