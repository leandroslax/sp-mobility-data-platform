# Databricks notebook source
# COMMAND ----------
# COMMAND ----------
%run ../00_setup/00_config
# COMMAND ----------
%run ../00_setup/01_adls_gen2_oauth_connection


# COMMAND ----------
# MAGIC %run ../00_setup/00_adls_gen2_oauth_connection
# COMMAND ----------


# COMMAND ----------



print("Landing extracted:", landing_extracted_path)

# COMMAND ----------

display(dbutils.fs.ls("abfss://bronze@stspmobilitydev001.dfs.core.windows.net/"))

# COMMAND ----------

routes_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{landing_extracted_path}/routes.txt")
)


# COMMAND ----------

stops_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{landing_extracted_path}/stops.txt")
)


# COMMAND ----------

trips_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{landing_extracted_path}/trips.txt")
)


# COMMAND ----------

stop_times_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{landing_extracted_path}/stop_times.txt")
)


# COMMAND ----------

calendar_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{landing_extracted_path}/calendar.txt")
)


# COMMAND ----------

shapes_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{landing_extracted_path}/shapes.txt")
)


# COMMAND ----------

