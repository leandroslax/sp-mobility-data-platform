from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

databases = [
    "sp_mobility_bronze",
    "sp_mobility_silver",
    "sp_mobility_gold",
    "sp_mobility_audit",
    "sp_mobility_quality"
]

for db in databases:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

print("Databases created successfully")
