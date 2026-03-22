# Databricks notebook source



# COMMAND ----------

print("🚀 Initializing Lakehouse structure...")

# COMMAND ----------



# COMMAND ----------

paths = [

    landing + "trips/",
    landing + "gps/",
    landing + "traffic/",
    landing + "incidents/",
    landing + "weather/",

    bronze + "trips/",
    bronze + "gps/",
    bronze + "traffic/",
    bronze + "incidents/",
    bronze + "weather/",

    silver + "trips/",
    silver + "gps/",
    silver + "traffic/",
    silver + "incidents/",
    silver + "weather/",

    gold + "trips_daily/",
    gold + "traffic_hourly/",
    gold + "incidents_by_type/",

    checkpoint + "trips/",
    checkpoint + "gps/",
    checkpoint + "traffic/",
    checkpoint + "schema/"
]

# COMMAND ----------

print("📁 Creating directories...")

for p in paths:
    try:
        dbutils.fs.mkdirs(p)
        print(f"✅ Created: {p}")
    except Exception as e:
        print(f"❌ Error creating {p}: {e}")

# COMMAND ----------

print("📂 Listing bronze container...")

try:
    files = dbutils.fs.ls(bronze)
    for f in files:
        print(f.path)
except Exception as e:
    print(f"❌ Error listing bronze: {e}")

# COMMAND ----------

print("🎯 Lakehouse structure created successfully!")
