# Databricks notebook source
# COMMAND ----------
# MAGIC %run ../00_setup/00_adls_gen2_oauth_connection
# COMMAND ----------
# Databricks notebook source

# COMMAND ----------

storage_account = "stspmobilitydev001"

landing_extracted_path = f"abfss://landing@{storage_account}.dfs.core.windows.net/gtfs/extracted"
bronze_base_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/gtfs"

print("Landing extracted:", landing_extracted_path)
print("Bronze base:", bronze_base_path)

# COMMAND ----------

dbutils.fs.mkdirs(bronze_base_path)
display(dbutils.fs.ls("abfss://bronze@stspmobilitydev001.dfs.core.windows.net/"))

# COMMAND ----------

routes_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{landing_extracted_path}/routes.txt")
)

routes_df.write.format("delta").mode("overwrite").save(f"{bronze_base_path}/gtfs_routes")

# COMMAND ----------

stops_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{landing_extracted_path}/stops.txt")
)

stops_df.write.format("delta").mode("overwrite").save(f"{bronze_base_path}/gtfs_stops")

# COMMAND ----------

trips_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{landing_extracted_path}/trips.txt")
)

trips_df.write.format("delta").mode("overwrite").save(f"{bronze_base_path}/gtfs_trips")

# COMMAND ----------

stop_times_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{landing_extracted_path}/stop_times.txt")
)

stop_times_df.write.format("delta").mode("overwrite").save(f"{bronze_base_path}/gtfs_stop_times")

# COMMAND ----------

calendar_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{landing_extracted_path}/calendar.txt")
)

calendar_df.write.format("delta").mode("overwrite").save(f"{bronze_base_path}/gtfs_calendar")

# COMMAND ----------

shapes_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{landing_extracted_path}/shapes.txt")
)

shapes_df.write.format("delta").mode("overwrite").save(f"{bronze_base_path}/gtfs_shapes")

# COMMAND ----------

display(dbutils.fs.ls(bronze_base_path))
