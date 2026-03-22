# Databricks notebook source

# MAGIC %run /Workspace/Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/notebooks/00_setup/00_config

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

print("🚀 Starting BRONZE GTFS processing...")

# COMMAND ----------

# Usando paths vindos do config
print("📂 Using paths from config...")

print(f"Base path: {base_path}")
print(f"Extract path: {extract_path}")
print(f"Bronze path: {bronze_path}")

# COMMAND ----------

# Read data (distributed)
print("📥 Reading GTFS files (distributed)...")

df = spark.read \
    .option("header", True) \
    .option("inferSchema", False) \
    .csv(f"{extract_path}/*.txt")

print(f"📊 Columns: {len(df.columns)}")

# COMMAND ----------

# Add metadata
print("🧠 Adding metadata...")

df = df.withColumn("ingestion_time", current_timestamp())

# COMMAND ----------

# Write Delta
print("💾 Writing to BRONZE (Delta)...")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(bronze_path)

# COMMAND ----------

print("✅ BRONZE GTFS completed successfully!")
