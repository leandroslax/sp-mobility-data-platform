# Databricks notebook source
# MAGIC %run "../00_setup/config"

config = load_config()

bronze_root = config["bronze_root"]
silver_root = config["silver_root"]
gold_root = config["gold_root"]

bronze_gtfs_routes = spark.read.format("delta").load(f"{bronze_root}/gtfs_routes")
bronze_gtfs_stops = spark.read.format("delta").load(f"{bronze_root}/gtfs_stops")
bronze_gtfs_trips = spark.read.format("delta").load(f"{bronze_root}/gtfs_trips")
bronze_gtfs_stop_times = spark.read.format("delta").load(f"{bronze_root}/gtfs_stop_times")
bronze_gtfs_calendar = spark.read.format("delta").load(f"{bronze_root}/gtfs_calendar")
bronze_gtfs_shapes = spark.read.format("delta").load(f"{bronze_root}/gtfs_shapes")
silver_gtfs_trips_enriched = spark.read.format("delta").load(
    config["gtfs_trips_enriched_path"]
)

gold_city_activity = spark.read.format("delta").load(config["city_activity_path"])
gold_route_performance = spark.read.format("delta").load(config["route_performance_path"])
gold_mobility_kpis = spark.read.format("delta").load(config["mobility_kpis_path"])
gold_city_heatmap = spark.read.format("delta").load(config["city_heatmap_path"])
gold_mobility_intelligence = spark.read.format("delta").load(
    config["mobility_intelligence_path"]
)

datasets = {
    "bronze_gtfs_routes": bronze_gtfs_routes,
    "bronze_gtfs_stops": bronze_gtfs_stops,
    "bronze_gtfs_trips": bronze_gtfs_trips,
    "bronze_gtfs_stop_times": bronze_gtfs_stop_times,
    "bronze_gtfs_calendar": bronze_gtfs_calendar,
    "bronze_gtfs_shapes": bronze_gtfs_shapes,
    "silver_gtfs_trips_enriched": silver_gtfs_trips_enriched,
    "gold_city_activity": gold_city_activity,
    "gold_route_performance": gold_route_performance,
    "gold_mobility_kpis": gold_mobility_kpis,
    "gold_city_heatmap": gold_city_heatmap,
    "gold_mobility_intelligence": gold_mobility_intelligence,
}

for name, df in datasets.items():
    df.createOrReplaceTempView(name)
    print(f"Temp view ready: {name}")
