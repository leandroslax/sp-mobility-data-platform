# Databricks notebook source

from pyspark.sql.functions import col

table_name = "sp_mobility_gold.city_activity"

df = spark.table(table_name)

row_count = df.count()
null_event_hour = df.filter(col("event_hour").isNull()).count()
invalid_event_hour = df.filter(
    (col("event_hour") < 0) | (col("event_hour") > 23)
).count()

print(f"Data quality checks for: {table_name}")
print(f"row_count: {row_count}")
print(f"null_event_hour: {null_event_hour}")
print(f"invalid_event_hour: {invalid_event_hour}")

if row_count == 0 or null_event_hour > 0 or invalid_event_hour > 0:
    raise Exception(
        f"Data quality validation failed for {table_name}. "
        f"row_count={row_count}, "
        f"null_event_hour={null_event_hour}, "
        f"invalid_event_hour={invalid_event_hour}"
    )

print(f"Data quality validation passed for {table_name}")

