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
    gold_root = f"abfss://gold@{storage_account}.dfs.core.windows.net"
    return {
        "gold_root": gold_root,
        "route_performance_path": f"{gold_root}/route_performance",
        "mobility_kpis_path": f"{gold_root}/mobility_kpis",
        "city_activity_path": f"{gold_root}/city_activity",
        "city_heatmap_path": f"{gold_root}/map/city_heatmap",
        "mobility_intelligence_path": f"{gold_root}/mobility/intelligence",
    }

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
