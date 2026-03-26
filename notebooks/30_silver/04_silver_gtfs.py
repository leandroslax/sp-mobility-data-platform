# Databricks notebook source

from pyspark.sql.functions import col, expr


def _get_widget(name, default_value):
    try:
        dbutils.widgets.text(name, default_value)
    except Exception:
        pass
    try:
        value = dbutils.widgets.get(name)
        return value or default_value
    except Exception:
        return default_value


def load_config():
    storage_account = _get_widget("storage_account", "stspmobilitydev001")
    bronze_root = f"abfss://bronze@{storage_account}.dfs.core.windows.net"
    silver_root = f"abfss://silver@{storage_account}.dfs.core.windows.net"
    gtfs_entities = {"routes": "gtfs_routes", "shapes": "gtfs_shapes", "trips": "gtfs_trips"}
    return {
        "gtfs_bronze_paths": {
            entity: f"{bronze_root}/{table_name}"
            for entity, table_name in gtfs_entities.items()
        },
        "gtfs_silver_shapes_path": f"{silver_root}/gtfs/shapes",
        "gtfs_trips_enriched_path": f"{silver_root}/gtfs_trips_enriched",
    }

config = load_config()

print("Starting SILVER GTFS processing...")

bronze_shapes_path = config["gtfs_bronze_paths"]["shapes"]
bronze_routes_path = config["gtfs_bronze_paths"]["routes"]
bronze_trips_path = config["gtfs_bronze_paths"]["trips"]

shapes_df = spark.read.format("delta").load(bronze_shapes_path)

silver_shapes_df = (
    shapes_df.select(
        col("shape_id"),
        expr("try_cast(shape_pt_lat as double)").alias("shape_pt_lat"),
        expr("try_cast(shape_pt_lon as double)").alias("shape_pt_lon"),
        expr("try_cast(shape_pt_sequence as int)").alias("shape_pt_sequence"),
        expr("try_cast(shape_dist_traveled as double)").alias(
            "shape_dist_traveled"
        ),
    )
    .dropna(subset=["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"])
    .dropDuplicates(["shape_id", "shape_pt_sequence"])
)

(
    silver_shapes_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(config["gtfs_silver_shapes_path"])
)

print(f"Wrote silver shapes to {config['gtfs_silver_shapes_path']}")

routes_df = spark.read.format("delta").load(bronze_routes_path)
trips_df = spark.read.format("delta").load(bronze_trips_path)

silver_trips_df = (
    trips_df.select(
        "route_id",
        "service_id",
        "trip_id",
        "trip_headsign",
        "direction_id",
        "shape_id",
    )
    .dropna(subset=["route_id", "service_id", "trip_id"])
    .dropDuplicates(["trip_id"])
    .join(
        routes_df.select(
            "route_id",
            "route_short_name",
            "route_long_name",
            "route_type",
        ).dropDuplicates(["route_id"]),
        on="route_id",
        how="left",
    )
)

(
    silver_trips_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(config["gtfs_trips_enriched_path"])
)

print(f"Wrote silver trips to {config['gtfs_trips_enriched_path']}")
print("SILVER GTFS completed.")
