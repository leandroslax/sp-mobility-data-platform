# Databricks notebook source

# COMMAND ----------
%run ../00_config

# COMMAND ----------

print("🚀 Creating lakehouse structure...")

# COMMAND ----------

base_path = f"abfss://bronze@{account_fqdn}"

folders = [
    "raw/gtfs",
    "raw/sptrans",
    "bronze/gtfs",
    "bronze/sptrans",
    "silver",
    "gold"
]

# COMMAND ----------

for folder in folders:
    path = f"{base_path}/{folder}"
    dbutils.fs.mkdirs(path)
    print(f"OK: {path}")

# COMMAND ----------

print("✅ Lakehouse structure created!")
