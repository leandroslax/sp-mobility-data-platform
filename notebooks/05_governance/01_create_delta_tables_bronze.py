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


def load_config():
    storage_account = _get_widget("storage_account", "stspmobilitydev001")
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
        "gtfs_entities": gtfs_entities,
        "gtfs_bronze_paths": {
            entity: f"{bronze_root}/{table_name}"
            for entity, table_name in gtfs_entities.items()
        },
    }

config = load_config()

for entity, path in config["gtfs_bronze_paths"].items():
    table_name = config["gtfs_entities"][entity]
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS sp_mobility_bronze.{table_name}
        USING DELTA
        LOCATION '{path}'
        """
    )
    print(f"Registered bronze table: sp_mobility_bronze.{table_name}")
