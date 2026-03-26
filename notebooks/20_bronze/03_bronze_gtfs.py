# Databricks notebook source
# Databricks notebook source

# ==========================================
# IMPORTS
# ==========================================

from pyspark.sql.functions import current_timestamp

print("🚀 Starting BRONZE GTFS processing...")

# ==========================================
# DEBUG CONFIG
# ==========================================

print("🔎 Validando config...")

print("Storage:", storage_account)
print("Base Path:", base_path)

# ==========================================
# SPARK CONFIG
# ==========================================

spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# ==========================================
# READ
# ==========================================

source_path = f"{gtfs_base_path}/delta"

print(f"📥 Reading from: {source_path}")

df = spark.read.format("delta").load(source_path)

print("✅ Data loaded")

# ==========================================
# TRANSFORM
# ==========================================

df = df.withColumn("ingestion_time", current_timestamp())

# ==========================================
# WRITE
# ==========================================

target_path = gtfs_bronze_path

print(f"📤 Writing to: {target_path}")

df.write.format("delta").mode("overwrite").save(target_path)

print("✅ BRONZE completed")

# ==========================================
# VALIDATION
# ==========================================

df_check = spark.read.format("delta").load(target_path)

print(f"📊 Total records: {df_check.count()}")

display(df_check.limit(10))

# COMMAND ----------

# ==========================================
# ==========================================

ENV = "dev"

container = "bronze"
storage_account = "stspmobilitydev001"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

gtfs_base_path = f"{base_path}/gtfs"
gtfs_bronze_path = f"{gtfs_base_path}/bronze"

# ==========================================
# IMPORTS
# ==========================================

from pyspark.sql.functions import current_timestamp

print("🚀 Starting BRONZE GTFS processing...")

print("Storage:", storage_account)
print("Base Path:", base_path)

# ==========================================
# SPARK CONFIG
# ==========================================

spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# ==========================================
# READ
# ==========================================

source_path = f"{gtfs_base_path}/delta"

print(f"📥 Reading from: {source_path}")

df = spark.read.format("delta").load(source_path)

print("✅ Data loaded")

# ==========================================
# TRANSFORM
# ==========================================

df = df.withColumn("ingestion_time", current_timestamp())

# ==========================================
# WRITE
# ==========================================

print(f"📤 Writing to: {gtfs_bronze_path}")

df.write.format("delta").mode("overwrite").save(gtfs_bronze_path)

print("✅ BRONZE completed")

# ==========================================
# VALIDATION
# ==========================================

df_check = spark.read.format("delta").load(gtfs_bronze_path)

print(f"📊 Total records: {df_check.count()}")

display(df_check.limit(10))
