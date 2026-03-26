# Databricks notebook source
# MAGIC %run "./config"

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
