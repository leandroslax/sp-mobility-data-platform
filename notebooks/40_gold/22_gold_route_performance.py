# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
silver_vehicle_path = "abfss://silver@stspmobilitydev001.dfs.core.windows.net/sptrans/vehicle_positions"
silver_gtfs_routes_path = "abfss://silver@stspmobilitydev001.dfs.core.windows.net/gtfs/routes"

gold_route_performance_path = "abfss://gold@stspmobilitydev001.dfs.core.windows.net/mobility/route_performance"

# COMMAND ----------

# COMMAND ----------
vehicle_positions_df = spark.read.format("delta").load(silver_vehicle_path)
routes_df = spark.read.format("delta").load(silver_gtfs_routes_path)

# COMMAND ----------

# COMMAND ----------
mobility_df = vehicle_positions_df.alias("vp").join(
    routes_df.alias("rt"),
    F.col("vp.line_code").cast("string") == F.col("rt.route_short_name").cast("string"),
    "left"
)

# COMMAND ----------

# COMMAND ----------
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

# COMMAND ----------

# COMMAND ----------
route_performance_df = mobility_intelligence_df.groupBy(
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
    F.countDistinct("vehicle_prefix").alias("active_vehicles"),
    F.count("*").alias("positions_received"),
    F.avg("latitude").alias("avg_latitude"),
    F.avg("longitude").alias("avg_longitude"),
    F.sum(F.when(F.col("accessible") == True, 1).otherwise(0)).alias("accessible_positions")
)

# COMMAND ----------

# COMMAND ----------
route_performance_df = route_performance_df.withColumn(
    "accessible_pct",
    F.round(
        (F.col("accessible_positions") / F.col("positions_received")) * 100,
        2
    )
)

display(route_performance_df.limit(20))

# COMMAND ----------

# COMMAND ----------
route_performance_df.write.format("delta") \
    .mode("overwrite") \
    .save(gold_route_performance_path)

# COMMAND ----------

# COMMAND ----------
route_performance_validation_df = spark.read.format("delta").load(gold_route_performance_path)

display(route_performance_validation_df.limit(20))
route_performance_validation_df.printSchema()

print("Total registros validados:", route_performance_validation_df.count())

# COMMAND ----------

# Databricks notebook source

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
silver_vehicle_path = "abfss://silver@stspmobilitydev001.dfs.core.windows.net/sptrans/vehicle_positions"
silver_gtfs_routes_path = "abfss://silver@stspmobilitydev001.dfs.core.windows.net/gtfs/routes"

gold_route_performance_path = "abfss://gold@stspmobilitydev001.dfs.core.windows.net/mobility/route_performance"

# COMMAND ----------
vehicle_positions_df = spark.read.format("delta").load(silver_vehicle_path)
routes_df = spark.read.format("delta").load(silver_gtfs_routes_path)

# COMMAND ----------
mobility_df = vehicle_positions_df.alias("vp").join(
    routes_df.alias("rt"),
    F.col("vp.line_code").cast("string") == F.col("rt.route_short_name").cast("string"),
    "left"
)

# COMMAND ----------
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

# COMMAND ----------
route_performance_df = mobility_intelligence_df.groupBy(
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
    F.countDistinct("vehicle_prefix").alias("active_vehicles"),
    F.count("*").alias("positions_received"),
    F.avg("latitude").alias("avg_latitude"),
    F.avg("longitude").alias("avg_longitude"),
    F.sum(F.when(F.col("accessible") == True, 1).otherwise(0)).alias("accessible_positions")
)

# COMMAND ----------
route_performance_df = route_performance_df.withColumn(
    "accessible_pct",
    F.round(
        (F.col("accessible_positions") / F.col("positions_received")) * 100,
        2
    )
)

display(route_performance_df.limit(20))

# COMMAND ----------
route_performance_df.write.format("delta") \
    .mode("overwrite") \
    .save(gold_route_performance_path)

# COMMAND ----------
route_performance_validation_df = spark.read.format("delta").load(gold_route_performance_path)

display(route_performance_validation_df.limit(20))
route_performance_validation_df.printSchema()

print("Total registros validados:", route_performance_validation_df.count())
