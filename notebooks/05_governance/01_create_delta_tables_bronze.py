# Databricks notebook source
# MAGIC %run /Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/notebooks/00_setup/config

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
