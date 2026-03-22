# Databricks notebook source

# COMMAND ----------

storage_account = "stspmobilitydev001"

landing = f"abfss://landing@{storage_account}.dfs.core.windows.net/"
bronze = f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
silver = f"abfss://silver@{storage_account}.dfs.core.windows.net/"
gold = f"abfss://gold@{storage_account}.dfs.core.windows.net/"
checkpoint = f"abfss://checkpoint@{storage_account}.dfs.core.windows.net/"

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

for p in paths:
    dbutils.fs.mkdirs(p)

print("Lakehouse structure created successfully")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@stspmobilitydev001.dfs.core.windows.net/")