# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
# MAGIC %run ../00_setup/00_config

# COMMAND ----------
# ==============================
# GOVERNANCE CATALOG REGISTRATION
# ==============================

print("🚀 Creating Hive Metastore databases...")

# Define databases and their metastore locations
databases = {
    "sp_mobility_bronze": f"{bronze_base_path}_metastore/sp_mobility_bronze",
    "sp_mobility_silver": f"{silver_base_path}_metastore/sp_mobility_silver",
    "sp_mobility_gold": f"{gold_base_path}_metastore/sp_mobility_gold",
    "sp_mobility_audit": f"{gold_base_path}_metastore/sp_mobility_audit",
    "sp_mobility_quality": f"{gold_base_path}_metastore/sp_mobility_quality"
}

for db_name, location in databases.items():
    print(f"📦 Creating database: {db_name} at {location}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{location}'")

print("✅ Hive Metastore databases created successfully")

# Show databases for validation
display(spark.sql("SHOW DATABASES"))
