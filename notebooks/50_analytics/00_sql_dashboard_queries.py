# Databricks notebook source
# COMMAND ----------
%run ../00_setup/00_config
# COMMAND ----------
%run ../00_setup/01_adls_gen2_oauth_connection



# COMMAND ----------



# COMMAND ----------

bronze_gtfs_routes = spark.read.format("delta").load(f"{bronze_path}/gtfs_routes")
bronze_gtfs_stops = spark.read.format("delta").load(f"{bronze_path}/gtfs_stops")
bronze_gtfs_trips = spark.read.format("delta").load(f"{bronze_path}/gtfs_trips")
bronze_gtfs_stop_times = spark.read.format("delta").load(f"{bronze_path}/gtfs_stop_times")
bronze_gtfs_calendar = spark.read.format("delta").load(f"{bronze_path}/gtfs_calendar")
bronze_gtfs_shapes = spark.read.format("delta").load(f"{bronze_path}/gtfs_shapes")

silver_gtfs_trips_enriched = spark.read.format("delta").load(f"{silver_path}/gtfs_trips_enriched")

gold_bus_lines_operational = spark.read.format("delta").load(f"{gold_mobility_path}/bus_lines_operational")
gold_bus_stops_geo = spark.read.format("delta").load(f"{gold_geo_path}/bus_stops_geo")
gold_bus_routes_geo = spark.read.format("delta").load(f"{gold_geo_path}/bus_routes_geo")
gold_bus_stops_map = spark.read.format("delta").load(f"{gold_map_path}/bus_stops_map")
gold_bus_routes_points_enriched = spark.read.format("delta").load(f"{gold_map_path}/bus_routes_points_enriched")
gold_bus_routes_wkt = spark.read.format("delta").load(f"{gold_map_path}/bus_routes_wkt")

# COMMAND ----------

bronze_gtfs_routes.createOrReplaceTempView("bronze_gtfs_routes")
bronze_gtfs_stops.createOrReplaceTempView("bronze_gtfs_stops")
bronze_gtfs_trips.createOrReplaceTempView("bronze_gtfs_trips")
bronze_gtfs_stop_times.createOrReplaceTempView("bronze_gtfs_stop_times")
bronze_gtfs_calendar.createOrReplaceTempView("bronze_gtfs_calendar")
bronze_gtfs_shapes.createOrReplaceTempView("bronze_gtfs_shapes")

silver_gtfs_trips_enriched.createOrReplaceTempView("silver_gtfs_trips_enriched")

gold_bus_lines_operational.createOrReplaceTempView("gold_bus_lines_operational")
gold_bus_stops_geo.createOrReplaceTempView("gold_bus_stops_geo")
gold_bus_routes_geo.createOrReplaceTempView("gold_bus_routes_geo")
gold_bus_stops_map.createOrReplaceTempView("gold_bus_stops_map")
gold_bus_routes_points_enriched.createOrReplaceTempView("gold_bus_routes_points_enriched")
gold_bus_routes_wkt.createOrReplaceTempView("gold_bus_routes_wkt")

print("Temp views criadas com sucesso.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM bronze_gtfs_routes) AS total_routes,
# MAGIC   (SELECT COUNT(*) FROM bronze_gtfs_stops) AS total_stops,
# MAGIC   (SELECT COUNT(*) FROM bronze_gtfs_trips) AS total_trips,
# MAGIC   (SELECT COUNT(*) FROM bronze_gtfs_shapes) AS total_shape_points

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   route_id,
# MAGIC   route_short_name,
# MAGIC   route_long_name,
# MAGIC   total_trips
# MAGIC FROM gold_bus_lines_operational
# MAGIC ORDER BY total_trips DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   stop_id,
# MAGIC   stop_name,
# MAGIC   latitude,
# MAGIC   longitude
# MAGIC FROM gold_bus_stops_map

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   shape_id,
# MAGIC   route_id,
# MAGIC   route_short_name,
# MAGIC   route_long_name,
# MAGIC   latitude,
# MAGIC   longitude,
# MAGIC   shape_pt_sequence
# MAGIC FROM gold_bus_routes_points_enriched

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   shape_id,
# MAGIC   route_id,
# MAGIC   route_short_name,
# MAGIC   route_long_name,
# MAGIC   wkt_linestring
# MAGIC FROM gold_bus_routes_wkt
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   stop_id,
# MAGIC   stop_name,
# MAGIC   latitude,
# MAGIC   longitude
# MAGIC FROM gold_bus_stops_map

# COMMAND ----------

# MAGIC %pip install folium

# COMMAND ----------

# restart_python_environment
dbutils.library.restartPython()

# COMMAND ----------

# reload_bus_stops_dataset



bus_stops_map = spark.read.format("delta").load(f"{gold_map_path}/bus_stops_map")

display(bus_stops_map.limit(5))

# COMMAND ----------

import folium

pdf = bus_stops_map.limit(500).toPandas()

map_sp = folium.Map(
    location=[-23.55, -46.63],
    zoom_start=11
)

for _, row in pdf.iterrows():
    folium.CircleMarker(
        location=[row["latitude"], row["longitude"]],
        radius=3,
        popup=row["stop_name"],
        color="blue",
        fill=True
    ).add_to(map_sp)

map_sp

# COMMAND ----------


