# Databricks notebook source
# MAGIC %run /Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/notebooks/00_setup/config

# COMMAND ----------

from pyspark.sql import functions as F

config = load_config()

city_activity_df = spark.read.format("delta").load(config["city_activity_path"])
route_performance_df = spark.read.format("delta").load(config["route_performance_path"])

vehicles_per_line_df = route_performance_df.groupBy("event_date", "event_hour").agg(
    F.avg("active_vehicles").alias("avg_vehicles_per_line"),
    F.max("active_vehicles").alias("max_vehicles_in_line"),
    F.sum("active_vehicles").alias("sum_active_vehicles_lines"),
)

mobility_kpis_df = (
    city_activity_df.alias("ca")
    .join(
        vehicles_per_line_df.alias("vl"),
        on=["event_date", "event_hour"],
        how="left",
    )
    .select(
        F.col("ca.event_date"),
        F.col("ca.event_hour"),
        F.col("ca.active_lines"),
        F.col("ca.active_vehicles").alias("total_vehicles"),
        F.col("ca.total_positions"),
        F.col("ca.accessible_positions"),
        F.col("ca.accessibility_pct"),
        F.round(F.col("vl.avg_vehicles_per_line"), 2).alias(
            "avg_vehicles_per_line"
        ),
        F.col("vl.max_vehicles_in_line"),
        F.col("vl.sum_active_vehicles_lines"),
    )
)

(
    mobility_kpis_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date")
    .save(config["mobility_kpis_path"])
)

print(f"Mobility KPIs refreshed at {config['mobility_kpis_path']}")
