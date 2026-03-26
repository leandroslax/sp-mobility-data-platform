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
    silver_root = f"abfss://silver@{storage_account}.dfs.core.windows.net"
    return {
        "gtfs_trips_enriched_path": f"{silver_root}/gtfs_trips_enriched",
        "sptrans_silver_path": f"{silver_root}/sptrans/vehicle_positions",
    }

config = load_config()

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS sp_mobility_silver.gtfs_trips_enriched
    USING DELTA
    LOCATION '{config["gtfs_trips_enriched_path"]}'
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS sp_mobility_silver.sptrans_vehicle_positions
    USING DELTA
    LOCATION '{config["sptrans_silver_path"]}'
    """
)

print("Silver tables registered.")
