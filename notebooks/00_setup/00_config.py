# Databricks notebook source

print("⚙️ Loading global configuration...")

# ==============================
# ENVIRONMENT CONFIG
# ==============================

container = "bronze"
storage_account = "stpmobilitydev001"

# ==============================
# PATHS
# ==============================

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

bronze_path = f"{base_path}/gtfs/bronze"
silver_path = f"{base_path}/gtfs/silver"
gold_path = f"{base_path}/gtfs/gold"

extract_path = f"{base_path}/gtfs/extracted"

# ==============================
# SPARK CONFIG (performance)
# ==============================

spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

print("⚡ Spark tuned")
print("✅ Config loaded successfully")