# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
# MAGIC %run ./00_adls_gen2_oauth_connection

# COMMAND ----------
# ==============================
# CREATE LAKEHOUSE STRUCTURE
# ==============================

print("🚀 Creating Lakehouse structure...")

try:
    # Create Base Containers (if they don't exist)
    # Note: dbutils.fs.mkdirs on abfss will fail if the container doesn't exist.
    # The containers should be created in the Azure Portal or via Terraform.
    # If they exist, mkdirs will just ensure the path is available.
    
    dbutils.fs.mkdirs(bronze_base_path)
    dbutils.fs.mkdirs(silver_base_path)
    dbutils.fs.mkdirs(gold_base_path)
    print("✅ Base containers verified")

    # ==============================
    # CREATE DOMAIN FOLDERS
    # ==============================
    print("📂 Creating domain folders...")

    # Bronze
    dbutils.fs.mkdirs(f"{bronze_base_path}/gtfs/raw/")
    dbutils.fs.mkdirs(f"{bronze_base_path}/sptrans/vehicle_positions/")

    # Silver
    dbutils.fs.mkdirs(f"{silver_base_path}/gtfs_trips_enriched/")
    dbutils.fs.mkdirs(f"{silver_base_path}/sptrans/vehicle_positions/")

    # Gold
    dbutils.fs.mkdirs(f"{gold_base_path}/analytics/")
    dbutils.fs.mkdirs(f"{gold_base_path}/mobility_intelligence/")
    dbutils.fs.mkdirs(f"{gold_base_path}/route_performance/")

    print("✅ Domain folders created")

    # ==============================
    # VALIDATION
    # ==============================
    print("🔍 Validating structure...")

    print("Bronze:")
    display(dbutils.fs.ls(bronze_base_path))

    print("Silver:")
    display(dbutils.fs.ls(silver_base_path))

    print("Gold:")
    display(dbutils.fs.ls(gold_base_path))

    print("🎯 Lakehouse structure created successfully!")

except Exception as e:
    print(f"❌ Error creating structure: {str(e)}")
    print("💡 Ensure the Service Principal has 'Storage Blob Data Contributor' permissions on the Storage Account.")
    raise e
