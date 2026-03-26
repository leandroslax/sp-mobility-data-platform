# Databricks notebook source
# MAGIC %run "../00_setup/config"

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
