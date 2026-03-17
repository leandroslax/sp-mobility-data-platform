# Databricks notebook source
storage_account = "stspmobilitydev001"

bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/gtfs"
gold_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/mobility_geo"

print(bronze_path)
print(gold_path)

# COMMAND ----------

dbutils.fs.mkdirs(gold_path)
dbutils.fs.ls(f"abfss://gold@{storage_account}.dfs.core.windows.net/")

# COMMAND ----------

stops_df = spark.read.format("delta").load(f"{bronze_path}/gtfs_stops")

display(stops_df)

# COMMAND ----------

from pyspark.sql.functions import col

stops_geo = (
    stops_df
    .select(
        col("stop_id"),
        col("stop_name"),
        col("stop_lat").alias("latitude"),
        col("stop_lon").alias("longitude")
    )
)

# COMMAND ----------

display(stops_geo)

# COMMAND ----------

stops_geo.write.format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/bus_stops_geo")

# COMMAND ----------

shapes_df = spark.read.format("delta").load(f"{bronze_path}/gtfs_shapes")

display(shapes_df)

# COMMAND ----------

from pyspark.sql.functions import col

routes_geo = (
    shapes_df
    .select(
        col("shape_id"),
        col("shape_pt_lat").alias("latitude"),
        col("shape_pt_lon").alias("longitude"),
        col("shape_pt_sequence")
    )
)

# COMMAND ----------

display(routes_geo)

# COMMAND ----------

routes_geo.write.format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/bus_routes_geo")

# COMMAND ----------

display(dbutils.fs.ls(gold_path))