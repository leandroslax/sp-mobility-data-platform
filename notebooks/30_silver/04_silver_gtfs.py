# Databricks notebook source

# COMMAND ----------

storage_account = "stspmobilitydev001"

bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/gtfs"
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/gtfs"

print(bronze_path)
print(silver_path)

# COMMAND ----------

routes_df = spark.read.format("delta").load(f"{bronze_path}/gtfs_routes")

trips_df = spark.read.format("delta").load(f"{bronze_path}/gtfs_trips")

stops_df = spark.read.format("delta").load(f"{bronze_path}/gtfs_stops")

# COMMAND ----------

trips_enriched = trips_df.join(
    routes_df,
    "route_id",
    "left"
)

# COMMAND ----------

display(trips_enriched)

# COMMAND ----------

dbutils.fs.mkdirs(silver_path)

# COMMAND ----------

trips_enriched.write.format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/gtfs_trips_enriched")

# COMMAND ----------

display(dbutils.fs.ls(silver_path))

# COMMAND ----------

display(dbutils.fs.ls("abfss://silver@stspmobilitydev001.dfs.core.windows.net/gtfs_trips_enriched"))
