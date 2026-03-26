# Databricks notebook source

from pyspark.sql.functions import current_timestamp


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


def load_config():
    storage_account = _get_widget("storage_account", "stspmobilitydev001")
    landing_root = f"abfss://landing@{storage_account}.dfs.core.windows.net"
    bronze_root = f"abfss://bronze@{storage_account}.dfs.core.windows.net"
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
        "gtfs_extract_paths": {
            entity: f"{landing_root}/gtfs/extracted/{entity}"
            for entity in gtfs_entities
        },
        "gtfs_bronze_paths": {
            entity: f"{bronze_root}/{table_name}"
            for entity, table_name in gtfs_entities.items()
        },
    }

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
