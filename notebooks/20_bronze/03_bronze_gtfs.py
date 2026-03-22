# Databricks notebook source
# MAGIC %run ../00_setup/00_config

# COMMAND ----------

print("🥉 Starting Bronze GTFS processing...")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

from pyspark.sql.functions import input_file_name

# Paths
storage_account = "stspmobilitydev001"
container = "bronze"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

raw_path = f"{base_path}/gtfs/extracted"
bronze_path = f"{base_path}/gtfs/bronze"

# COMMAND ----------

print("📂 Reading extracted files...")

df = spark.read \
    .option("header", True) \
    .option("inferSchema", False) \
    .csv(f"{raw_path}/*.txt") \
    .withColumn("source_file", input_file_name())

# COMMAND ----------

print("⚙️ Repartitioning...")

df = df.repartition(8)

# COMMAND ----------

print("📊 Writing Bronze layer (partitioned by file)...")

df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("source_file") \
    .save(bronze_path)

# COMMAND ----------

print("🎯 Bronze layer completed!")
