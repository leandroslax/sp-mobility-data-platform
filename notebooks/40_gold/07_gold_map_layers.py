# Databricks notebook source
# MAGIC %run "../00_setup/config"

from pyspark.sql.functions import col, expr

config = load_config()

print("Starting map layers generation...")

df = spark.read.format("delta").load(f"{config['gold_root']}/gtfs/geo_routes")

geojson_df = df.select(
    col("shape_id"),
    expr(
        """
        to_json(
            named_struct(
                'type', 'LineString',
                'coordinates',
                transform(points, p -> array(p.shape_pt_lon, p.shape_pt_lat))
            )
        )
        """
    ).alias("geojson"),
)

(
    geojson_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{config['gold_root']}/gtfs/geojson_routes")
)

print("Map layers completed.")
