# Databricks notebook source
def load_config():
    ENV = "dev"

    container = "bronze"
    storage_account = "stspmobilitydev001"

    base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

    gtfs_base_path = f"{base_path}/gtfs"

    gtfs_extract_path = f"{gtfs_base_path}/extracted"
    gtfs_bronze_path  = f"{gtfs_base_path}/bronze"
    gtfs_silver_path  = f"{gtfs_base_path}/silver"
    gtfs_gold_path    = f"{gtfs_base_path}/gold"

    return {
        "ENV": ENV,
        "container": container,
        "storage_account": storage_account,
        "base_path": base_path,
        "gtfs_base_path": gtfs_base_path,
        "gtfs_extract_path": gtfs_extract_path,
        "gtfs_bronze_path": gtfs_bronze_path,
        "gtfs_silver_path": gtfs_silver_path,
        "gtfs_gold_path": gtfs_gold_path
    }
