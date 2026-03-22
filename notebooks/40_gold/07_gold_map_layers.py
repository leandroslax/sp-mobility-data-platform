# Databricks notebook source

# COMMAND ----------

storage_account = "stspmobilitydev001"

bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/gtfs"
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/gtfs"
gold_geo_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/mobility_geo"
gold_map_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/mobility_map"

print("Bronze:", bronze_path)
print("Silver:", silver_path)
print("Gold geo:", gold_geo_path)
print("Gold map:", gold_map_path)

# COMMAND ----------

dbutils.fs.mkdirs(gold_map_path)

display(dbutils.fs.ls("abfss://gold@stspmobilitydev001.dfs.core.windows.net/"))

# COMMAND ----------

from pyspark.sql.functions import col

stops_geo_df = spark.read.format("delta").load(f"{gold_geo_path}/bus_stops_geo")
routes_geo_df = spark.read.format("delta").load(f"{gold_geo_path}/bus_routes_geo")

trips_enriched_df = spark.read.format("delta").load(f"{silver_path}/gtfs_trips_enriched")

# COMMAND ----------

bus_stops_map = (
    stops_geo_df
    .select(
        col("stop_id"),
        col("stop_name"),
        col("latitude"),
        col("longitude")
    )
)

display(bus_stops_map)

# COMMAND ----------

bus_stops_map.write.format("delta") \
    .mode("overwrite") \
    .save(f"{gold_map_path}/bus_stops_map")

# COMMAND ----------

route_shapes = (
    trips_enriched_df
    .select(
        col("shape_id"),
        col("route_id"),
        col("route_short_name"),
        col("route_long_name")
    )
    .dropna(subset=["shape_id"])
    .dropDuplicates(["shape_id", "route_id"])
)

# COMMAND ----------

bus_routes_points_enriched = (
    routes_geo_df.alias("g")
    .join(route_shapes.alias("r"), on="shape_id", how="left")
    .select(
        col("shape_id"),
        col("route_id"),
        col("route_short_name"),
        col("route_long_name"),
        col("latitude"),
        col("longitude"),
        col("shape_pt_sequence")
    )
)

display(bus_routes_points_enriched)

# COMMAND ----------

bus_routes_points_enriched.write.format("delta") \
    .mode("overwrite") \
    .save(f"{gold_map_path}/bus_routes_points_enriched")

# COMMAND ----------

from pyspark.sql.functions import expr

bus_routes_wkt = (
    bus_routes_points_enriched
    .groupBy("shape_id", "route_id", "route_short_name", "route_long_name")
    .agg(
        expr("""
            array_sort(
                collect_list(
                    named_struct(
                        'seq', shape_pt_sequence,
                        'lon', longitude,
                        'lat', latitude
                    )
                )
            ) as ordered_points
        """)
    )
    .withColumn(
        "wkt_linestring",
        expr("""
            concat(
                'LINESTRING(',
                array_join(
                    transform(
                        ordered_points,
                        p -> concat(cast(p.lon as string), ' ', cast(p.lat as string))
                    ),
                    ', '
                ),
                ')'
            )
        """)
    )
)

# COMMAND ----------

display(
    bus_routes_wkt.select(
        "shape_id",
        "route_id",
        "route_short_name",
        "route_long_name",
        "wkt_linestring"
    )
)

# COMMAND ----------

bus_routes_wkt.write.format("delta") \
    .mode("overwrite") \
    .save(f"{gold_map_path}/bus_routes_wkt")

# COMMAND ----------

display(dbutils.fs.ls(gold_map_path))

# COMMAND ----------

display(bus_stops_map)
