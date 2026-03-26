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
    return {
        "bronze_root": f"abfss://bronze@{storage_account}.dfs.core.windows.net",
        "silver_root": f"abfss://silver@{storage_account}.dfs.core.windows.net",
        "gold_root": f"abfss://gold@{storage_account}.dfs.core.windows.net",
    }

config = load_config()

print("Registering governance databases...")

spark.sql(
    f"""
    CREATE DATABASE IF NOT EXISTS sp_mobility_bronze
    LOCATION '{config["bronze_root"]}/_metastore/sp_mobility_bronze'
    """
)

spark.sql(
    f"""
    CREATE DATABASE IF NOT EXISTS sp_mobility_silver
    LOCATION '{config["silver_root"]}/_metastore/sp_mobility_silver'
    """
)

spark.sql(
    f"""
    CREATE DATABASE IF NOT EXISTS sp_mobility_gold
    LOCATION '{config["gold_root"]}/_metastore/sp_mobility_gold'
    """
)

spark.sql(
    f"""
    CREATE DATABASE IF NOT EXISTS sp_mobility_audit
    LOCATION '{config["gold_root"]}/_metastore/sp_mobility_audit'
    """
)

spark.sql(
    f"""
    CREATE DATABASE IF NOT EXISTS sp_mobility_quality
    LOCATION '{config["gold_root"]}/_metastore/sp_mobility_quality'
    """
)

print("Governance catalog registration completed.")
