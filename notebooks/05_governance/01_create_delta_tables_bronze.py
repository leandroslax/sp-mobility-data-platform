# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
# MAGIC %run ../00_setup/00_config

# COMMAND ----------
# ==============================
# CREATE DELTA TABLES BRONZE
# ==============================

print("🚀 Registering Bronze Delta tables...")

# Create Database if not exists
spark.sql("CREATE DATABASE IF NOT EXISTS sp_mobility_bronze")

# List of GTFS tables to register
gtfs_tables = [
    "gtfs_routes", "gtfs_trips", "gtfs_stops", 
    "gtfs_stop_times", "gtfs_calendar", "gtfs_shapes"
]

for table in gtfs_tables:
    location = f"{bronze_base_path}gtfs/{table}"
    print(f"📦 Registering table: {table} at {location}")
    
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS sp_mobility_bronze.{table}
    USING DELTA
    LOCATION '{location}'
    """)

# SPTrans Vehicle Positions Table
sptrans_table = "sptrans_vehicle_positions"
sptrans_location = f"{bronze_base_path}sptrans/{sptrans_table}"
print(f"📦 Registering table: {sptrans_table} at {sptrans_location}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS sp_mobility_bronze.{sptrans_table}
USING DELTA
LOCATION '{sptrans_location}'
""")

print("✅ Bronze tables registered successfully!")
