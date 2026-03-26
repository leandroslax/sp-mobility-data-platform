# Databricks notebook source
# MAGIC %run "../00_setup/config"

config = load_config()

print("Registering databases and tables in Hive Metastore...")

spark.sql(
    f"""
    CREATE DATABASE IF NOT EXISTS sp_mobility_bronze
    LOCATION '{config["bronze_root"]}/'
    """
)

spark.sql(
    f"""
    CREATE DATABASE IF NOT EXISTS sp_mobility_silver
    LOCATION '{config["silver_root"]}/'
    """
)

spark.sql(
    f"""
    CREATE DATABASE IF NOT EXISTS sp_mobility_gold
    LOCATION '{config["gold_root"]}/'
    """
)

spark.sql(
    f"""
    CREATE DATABASE IF NOT EXISTS sp_mobility_audit
    LOCATION '{config["gold_root"]}/audit/'
    """
)

spark.sql(
    f"""
    CREATE DATABASE IF NOT EXISTS sp_mobility_quality
    LOCATION '{config["quality_path"]}/'
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS sp_mobility_silver.sptrans_vehicle_positions
    USING DELTA
    LOCATION '{config["sptrans_silver_path"]}'
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS sp_mobility_silver.gtfs_trips_enriched
    USING DELTA
    LOCATION '{config["gtfs_trips_enriched_path"]}'
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS sp_mobility_audit.pipeline_runs
    USING DELTA
    LOCATION '{config["pipeline_runs_path"]}'
    """
)

print("Observability catalog registration completed.")
