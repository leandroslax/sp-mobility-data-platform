
storage_account = "stspmobilitydev001"

landing_path = f"abfss://landing@{storage_account}.dfs.core.windows.net/sptrans/vehicle_positions"
bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/sptrans/vehicle_positions"

dbutils.fs.mkdirs(bronze_path)

print("Landing:", landing_path)
print("Bronze:", bronze_path)

df_raw = spark.read.json(landing_path)

display(df_raw)

from pyspark.sql.functions import explode

df_lines = df_raw.select(
    "hr",
    explode("l").alias("line")
)

from pyspark.sql.functions import col, explode

df_vehicles = df_lines.select(
    col("hr"),
    col("line.c").alias("line_code"),
    col("line.cl").alias("line_id"),
    col("line.lt0").alias("line_name"),
    explode(col("line.vs")).alias("vehicle")
)

from pyspark.sql.functions import current_timestamp

df_bronze = df_vehicles.select(
    "hr",
    "line_code",
    "line_id",
    "line_name",
    col("vehicle.p").alias("vehicle_prefix"),
    col("vehicle.a").alias("accessible"),
    col("vehicle.ta").alias("timestamp_api"),
    col("vehicle.py").alias("latitude"),
    col("vehicle.px").alias("longitude")
).withColumn(
    "ingestion_timestamp",
    current_timestamp()
)

display(df_bronze)

df_bronze.write \
    .format("delta") \
    .mode("append") \
    .save(bronze_path)

print("Dados gravados na Bronze")

df_check = spark.read.format("delta").load(bronze_path)

display(df_check)
