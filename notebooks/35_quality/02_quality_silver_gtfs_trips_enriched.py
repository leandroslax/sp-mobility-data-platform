# Databricks notebook source

from pyspark.sql.functions import col

table_name = "sp_mobility_silver.gtfs_trips_enriched"

df = spark.table(table_name)

row_count = df.count()
null_route_id = df.filter(col("route_id").isNull()).count()
null_trip_id = df.filter(col("trip_id").isNull()).count()
null_service_id = df.filter(col("service_id").isNull()).count()

duplicate_trip_id = (
    df.groupBy("trip_id").count().filter(col("count") > 1).count()
)

print(f"Data quality checks for: {table_name}")
print(f"row_count: {row_count}")
print(f"null_route_id: {null_route_id}")
print(f"null_trip_id: {null_trip_id}")
print(f"null_service_id: {null_service_id}")
print(f"duplicate_trip_id: {duplicate_trip_id}")

if (
    row_count == 0
    or null_route_id > 0
    or null_trip_id > 0
    or null_service_id > 0
    or duplicate_trip_id > 0
):
    raise Exception(
        f"Data quality validation failed for {table_name}. "
        f"row_count={row_count}, "
        f"null_route_id={null_route_id}, "
        f"null_trip_id={null_trip_id}, "
        f"null_service_id={null_service_id}, "
        f"duplicate_trip_id={duplicate_trip_id}"
    )

print(f"Data quality validation passed for {table_name}")

