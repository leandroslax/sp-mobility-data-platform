# Databricks notebook source
# MAGIC %run ../00_setup/config

config = load_config()

gold_tables = {
    "mobility_kpis": config["mobility_kpis_path"],
    "city_activity": f"{config['gold_root']}/city_activity",
    "route_performance": config["route_performance_path"],
    "city_heatmap": f"{config['gold_root']}/map/city_heatmap",
    "mobility_intelligence": f"{config['gold_root']}/mobility/intelligence",
}

for table_name, path in gold_tables.items():
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS sp_mobility_gold.{table_name}
        USING DELTA
        LOCATION '{path}'
        """
    )
    print(f"Registered gold table: sp_mobility_gold.{table_name}")
