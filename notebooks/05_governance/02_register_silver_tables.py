# Databricks notebook source
# MAGIC %run /Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/notebooks/00_setup/config

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
