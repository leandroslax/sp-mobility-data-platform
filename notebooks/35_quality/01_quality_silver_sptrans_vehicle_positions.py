# Databricks notebook source

from pyspark.sql.functions import col

table_name = "sp_mobility_silver.sptrans_vehicle_positions"

df = spark.table(table_name)

null_vehicle_prefix = df.filter(col("vehicle_prefix").isNull()).count()
null_timestamp_api = df.filter(col("timestamp_api").isNull()).count()
null_latitude = df.filter(col("latitude").isNull()).count()
null_longitude = df.filter(col("longitude").isNull()).count()
invalid_event_hour = df.filter(
    (col("event_hour") < 0) | (col("event_hour") > 23)
).count()

print(f"Data quality checks for: {table_name}")
print(f"null_vehicle_prefix: {null_vehicle_prefix}")
print(f"null_timestamp_api: {null_timestamp_api}")
print(f"null_latitude: {null_latitude}")
print(f"null_longitude: {null_longitude}")
print(f"invalid_event_hour: {invalid_event_hour}")

if (
    null_vehicle_prefix > 0
    or null_timestamp_api > 0
    or null_latitude > 0
    or null_longitude > 0
    or invalid_event_hour > 0
):
    raise Exception(
        f"Data quality validation failed for {table_name}. "
        f"null_vehicle_prefix={null_vehicle_prefix}, "
        f"null_timestamp_api={null_timestamp_api}, "
        f"null_latitude={null_latitude}, "
        f"null_longitude={null_longitude}, "
        f"invalid_event_hour={invalid_event_hour}"
    )

print(f"Data quality validation passed for {table_name}")

