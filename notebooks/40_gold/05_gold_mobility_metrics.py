# Databricks
%run ../00_setup/00_config

# Databricks notebook source


# COMMAND ----------



print(bronze_path)
print(silver_path)
print(gold_path)

# COMMAND ----------

trips_df = spark.read.format("delta").load(f"{silver_path}/gtfs_trips_enriched")

# COMMAND ----------

from pyspark.sql.functions import count

trips_per_route = (
    trips_df.groupBy("route_id", "route_short_name", "route_long_name")
    .agg(count("trip_id").alias("total_trips"))
)

# COMMAND ----------

display(trips_per_route.orderBy("total_trips", ascending=False))

# COMMAND ----------

dbutils.fs.mkdirs(gold_path)

trips_per_route.write.format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/bus_lines_operational")

# COMMAND ----------

display(dbutils.fs.ls(gold_path))
