# Databricks notebook source

from pyspark.sql.functions import col

table_name = "sp_mobility_gold.mobility_kpis"

df = spark.table(table_name)

row_count = df.count()
null_event_date = df.filter(col("event_date").isNull()).count()
null_event_hour = df.filter(col("event_hour").isNull()).count()
null_total_vehicles = df.filter(col("total_vehicles").isNull()).count()
null_total_positions = df.filter(col("total_positions").isNull()).count()
invalid_event_hour = df.filter(
    (col("event_hour") < 0) | (col("event_hour") > 23)
).count()
negative_total_vehicles = df.filter(col("total_vehicles") < 0).count()
negative_total_positions = df.filter(col("total_positions") < 0).count()

print(f"Data quality checks for: {table_name}")
print(f"row_count: {row_count}")
print(f"null_event_date: {null_event_date}")
print(f"null_event_hour: {null_event_hour}")
print(f"null_total_vehicles: {null_total_vehicles}")
print(f"null_total_positions: {null_total_positions}")
print(f"invalid_event_hour: {invalid_event_hour}")
print(f"negative_total_vehicles: {negative_total_vehicles}")
print(f"negative_total_positions: {negative_total_positions}")

if (
    row_count == 0
    or null_event_date > 0
    or null_event_hour > 0
    or null_total_vehicles > 0
    or null_total_positions > 0
    or invalid_event_hour > 0
    or negative_total_vehicles > 0
    or negative_total_positions > 0
):
    raise Exception(
        f"Data quality validation failed for {table_name}. "
        f"row_count={row_count}, "
        f"null_event_date={null_event_date}, "
        f"null_event_hour={null_event_hour}, "
        f"null_total_vehicles={null_total_vehicles}, "
        f"null_total_positions={null_total_positions}, "
        f"invalid_event_hour={invalid_event_hour}, "
        f"negative_total_vehicles={negative_total_vehicles}, "
        f"negative_total_positions={negative_total_positions}"
    )

print(f"Data quality validation passed for {table_name}")
