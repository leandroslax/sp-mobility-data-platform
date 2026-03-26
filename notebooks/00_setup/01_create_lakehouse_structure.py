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
    landing_root = f"abfss://landing@{storage_account}.dfs.core.windows.net"
    bronze_root = f"abfss://bronze@{storage_account}.dfs.core.windows.net"
    silver_root = f"abfss://silver@{storage_account}.dfs.core.windows.net"
    gold_root = f"abfss://gold@{storage_account}.dfs.core.windows.net"
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
        "gtfs_extract_path": f"{landing_root}/gtfs/extracted",
        "sptrans_landing_path": f"{landing_root}/sptrans/vehicle_positions",
        "sptrans_bronze_path": f"{bronze_root}/sptrans/vehicle_positions",
        "sptrans_silver_path": f"{silver_root}/sptrans/vehicle_positions",
        "sptrans_gold_path": f"{gold_root}/sptrans/vehicle_positions",
        "route_performance_path": f"{gold_root}/route_performance",
        "pipeline_audit_path": f"{gold_root}/audit/pipeline_audit",
        "pipeline_runs_path": f"{gold_root}/audit/pipeline_runs",
        "quality_path": f"{gold_root}/quality",
        "gtfs_silver_shapes_path": f"{silver_root}/gtfs/shapes",
        "gtfs_trips_enriched_path": f"{silver_root}/gtfs_trips_enriched",
        "gtfs_bronze_paths": {
            entity: f"{bronze_root}/{table_name}"
            for entity, table_name in gtfs_entities.items()
        },
    }

config = load_config()

paths_to_create = [
    config["gtfs_extract_path"],
    config["sptrans_landing_path"],
    config["sptrans_bronze_path"],
    config["sptrans_silver_path"],
    config["sptrans_gold_path"],
    config["route_performance_path"],
    config["pipeline_audit_path"],
    config["pipeline_runs_path"],
    config["quality_path"],
    config["gtfs_silver_shapes_path"],
    config["gtfs_trips_enriched_path"],
]

paths_to_create.extend(config["gtfs_bronze_paths"].values())

print("Creating lakehouse structure...")

for path in paths_to_create:
    dbutils.fs.mkdirs(path)
    print(f"OK: {path}")

print("Lakehouse structure created successfully.")
