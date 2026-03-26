# Databricks notebook source
print("⚙️ Loading global configuration...")

# ==============================
# ENVIRONMENT CONFIG
# ==============================

ENV = "dev"

container = "bronze"
storage_account = "stpmobilitydev001"

# ==============================
# BASE PATH (ADLS GEN2 CORRETO)
# ==============================

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

# ==============================
# DOMAIN: GTFS (STATIC DATA)
# ==============================

gtfs_base_path = f"{base_path}/gtfs"

gtfs_extract_path = f"{gtfs_base_path}/extracted"
gtfs_bronze_path  = f"{gtfs_base_path}/bronze"
gtfs_silver_path  = f"{gtfs_base_path}/silver"
gtfs_gold_path    = f"{gtfs_base_path}/gold"

# ==============================
# DOMAIN: SPTRANS (REALTIME)
# ==============================

sptrans_base_path = f"{base_path}/sptrans"

landing_path = f"{sptrans_base_path}/landing"
sptrans_bronze_path = f"{sptrans_base_path}/bronze"
sptrans_silver_path = f"{sptrans_base_path}/silver"
sptrans_gold_path   = f"{sptrans_base_path}/gold"

# ==============================
# FILE CONTROL
# ==============================

date_partition_format = "yyyy/MM/dd"
timestamp_format = "yyyy-MM-dd HH:mm:ss"

# ==============================
# SPARK CONFIG
# ==============================

spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# ==============================
# DEBUG
# ==============================

print(f"🌍 Environment: {ENV}")
print(f"📦 Storage: {storage_account}")
print(f"📁 Base Path: {base_path}")

print("🚌 SPTrans Paths:")
print(f"  - Landing: {landing_path}")

# ==============================
# TESTE DATA LAKE
# ==============================

print("🔎 Validando acesso ao Data Lake...")

try:
    display(dbutils.fs.ls(base_path))
    print("✅ Data Lake acessível")

except Exception as e:
    print("❌ Erro ao acessar Data Lake:")
    print(e)
