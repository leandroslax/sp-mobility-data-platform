# Databricks notebook source
# MAGIC %run ./00_adls_gen2_oauth_connection

# COMMAND ----------

storage_account = "stspmobilitydev001"

raw_zip_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/raw/gtfs/cittamobi_gtfs.zip"
bronze_gtfs_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/gtfs"

local_zip = "/tmp/cittamobi_gtfs.zip"
extract_dir = "/tmp/gtfs"

print("RAW ZIP path:", raw_zip_path)
print("Bronze GTFS path:", bronze_gtfs_path)
print("Local ZIP:", local_zip)
print("Extract dir:", extract_dir)

# COMMAND ----------

display(dbutils.fs.ls("abfss://bronze@stspmobilitydev001.dfs.core.windows.net/raw/gtfs/"))
display(dbutils.fs.ls(bronze_gtfs_path))

# COMMAND ----------

display(dbutils.fs.ls("abfss://bronze@stspmobilitydev001.dfs.core.windows.net/raw/gtfs/"))
display(dbutils.fs.ls(bronze_gtfs_path))

# COMMAND ----------

dbutils.fs.cp(raw_zip_path, f"file:{local_zip}", True)
print("Arquivo copiado para:", local_zip)

# COMMAND ----------

import os
import zipfile
import shutil

if os.path.exists(extract_dir):
    shutil.rmtree(extract_dir)
os.makedirs(extract_dir, exist_ok=True)

with zipfile.ZipFile(local_zip, "r") as zip_ref:
    zip_ref.extractall(extract_dir)

print("Arquivos extraídos:")
print(sorted(os.listdir(extract_dir)))

# COMMAND ----------

for file_name in os.listdir(extract_dir):
    local_file = f"{extract_dir}/{file_name}"
    target_file = f"{bronze_gtfs_path}/{file_name}"
    dbutils.fs.cp(f"file:{local_file}", target_file, True)

print("Arquivos enviados para bronze/gtfs")
display(dbutils.fs.ls(bronze_gtfs_path))

# COMMAND ----------

routes_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{bronze_gtfs_path}/routes.txt")
)

display(routes_df)

# COMMAND ----------

trips_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{bronze_gtfs_path}/trips.txt")
)

display(trips_df)

# COMMAND ----------

stops_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{bronze_gtfs_path}/stops.txt")
)

display(stops_df)

# COMMAND ----------

stop_times_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{bronze_gtfs_path}/stop_times.txt")
)

display(stop_times_df)

# COMMAND ----------

calendar_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{bronze_gtfs_path}/calendar.txt")
)

display(calendar_df)

# COMMAND ----------

shapes_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{bronze_gtfs_path}/shapes.txt")
)

display(shapes_df)

# COMMAND ----------

routes_df.write.format("delta").mode("overwrite").save("abfss://bronze@stspmobilitydev001.dfs.core.windows.net/gtfs_routes")
trips_df.write.format("delta").mode("overwrite").save("abfss://bronze@stspmobilitydev001.dfs.core.windows.net/gtfs_trips")
stops_df.write.format("delta").mode("overwrite").save("abfss://bronze@stspmobilitydev001.dfs.core.windows.net/gtfs_stops")
stop_times_df.write.format("delta").mode("overwrite").save("abfss://bronze@stspmobilitydev001.dfs.core.windows.net/gtfs_stop_times")
calendar_df.write.format("delta").mode("overwrite").save("abfss://bronze@stspmobilitydev001.dfs.core.windows.net/gtfs_calendar")
shapes_df.write.format("delta").mode("overwrite").save("abfss://bronze@stspmobilitydev001.dfs.core.windows.net/gtfs_shapes")

print("GTFS Bronze Delta gravado com sucesso")

# COMMAND ----------

display(dbutils.fs.ls("abfss://bronze@stspmobilitydev001.dfs.core.windows.net/"))

# COMMAND ----------

spark.sql("SHOW TABLES IN sp_mobility_bronze").show()

# COMMAND ----------

spark.table("sp_mobility_bronze.gtfs_routes").show(5)
