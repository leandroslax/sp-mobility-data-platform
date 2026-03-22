# Databricks notebook source
# COMMAND ----------
%run ../00_setup/00_config
# COMMAND ----------
%run ../00_setup/01_adls_gen2_oauth_connection


# COMMAND ----------
# MAGIC %run ../00_setup/00_adls_gen2_oauth_connection
# COMMAND ----------
bronze_gtfs_routes_path = "abfss://bronze@stspmobilitydev001.dfs.core.windows.net/gtfs/gtfs_routes"
silver_gtfs_routes_path = "abfss://silver@stspmobilitydev001.dfs.core.windows.net/gtfs/routes"

routes_bronze_df = spark.read.format("delta").load(bronze_gtfs_routes_path)

display(routes_bronze_df.limit(10))
routes_bronze_df.printSchema()
print("Total bronze routes:", routes_bronze_df.count())

from pyspark.sql import functions as F

routes_silver_df = (
    routes_bronze_df
    .dropDuplicates()
)

routes_silver_df.write.format("delta") \
    .mode("overwrite") \
    .save(silver_gtfs_routes_path)

routes_df = spark.read.format("delta").load(silver_gtfs_routes_path)

display(routes_df.limit(10))
routes_df.printSchema()
print("Total silver routes:", routes_df.count())

from pyspark.sql import functions as F

silver_vehicle_path = "abfss://silver@stspmobilitydev001.dfs.core.windows.net/sptrans/vehicle_positions"
silver_gtfs_routes_path = "abfss://silver@stspmobilitydev001.dfs.core.windows.net/gtfs/routes"
gold_mobility_path = "abfss://gold@stspmobilitydev001.dfs.core.windows.net/mobility/intelligence"

vehicle_positions_df = spark.read.format("delta").load(silver_vehicle_path)
routes_df = spark.read.format("delta").load(silver_gtfs_routes_path)

vehicle_positions_df.printSchema()
routes_df.printSchema()

display(
    vehicle_positions_df.select("line_code", "line_id", "line_name").distinct().limit(20)
)

display(
    routes_df.select("route_id", "route_short_name", "route_long_name").distinct().limit(20)
)

mobility_df = vehicle_positions_df.alias("vp").join(
    routes_df.alias("rt"),
    F.col("vp.line_code").cast("string") == F.col("rt.route_short_name").cast("string"),
    "left"
)

display(
    mobility_df.select(
        "vp.line_code",
        "vp.line_name",
        "rt.route_short_name",
        "rt.route_long_name"
    ).limit(20)
)

mobility_intelligence_df = mobility_df.select(
    F.col("vp.line_code"),
    F.col("vp.line_id"),
    F.col("vp.line_name"),
    F.col("vp.vehicle_prefix"),
    F.col("vp.accessible"),
    F.col("vp.timestamp_api"),
    F.col("vp.latitude"),
    F.col("vp.longitude"),
    F.col("vp.event_date"),
    F.col("vp.event_hour"),
    F.col("rt.route_id"),
    F.col("rt.route_short_name"),
    F.col("rt.route_long_name"),
    F.col("rt.route_type")
)

display(mobility_intelligence_df.limit(20))

gold_mobility_df = mobility_intelligence_df.groupBy(
    "event_date",
    "event_hour",
    "line_code",
    "line_id",
    "line_name",
    "route_id",
    "route_short_name",
    "route_long_name",
    "route_type"
).agg(
    F.countDistinct("vehicle_prefix").alias("active_vehicles")
)

display(gold_mobility_df.limit(20))

gold_mobility_df.write.format("delta") \
    .mode("overwrite") \
    .save(gold_mobility_path)

gold_validation_df = spark.read.format("delta").load(gold_mobility_path)

display(gold_validation_df.limit(20))

gold_validation_df.printSchema()

print("Total registros gold:", gold_validation_df.count())
