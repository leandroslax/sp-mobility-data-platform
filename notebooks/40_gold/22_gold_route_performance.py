# Databricks notebook source
# Databricks notebook source
# MAGIC %run ../00_setup/00_adls_gen2_oauth_connection

from pyspark.sql import functions as F

# COMMAND ----------

silver_table = "sp_mobility_silver.sptrans_vehicle_positions"
gold_table = "sp_mobility_gold.route_performance"
gold_path = "abfss://gold@stspmobilitydev001.dfs.core.windows.net/route_performance"

# COMMAND ----------

# COMMAND ----------

df = spark.table(silver_table)

print("COUNT source df:", df.count())
df.printSchema()

# COMMAND ----------

# COMMAND ----------

df_clean = (
    df.filter(F.col("line_code").isNotNull())
      .filter(F.col("vehicle_prefix").isNotNull())
      .filter(F.col("event_date").isNotNull())
      .filter(F.col("event_hour").isNotNull())
)

print("COUNT df_clean:", df_clean.count())

# COMMAND ----------

# COMMAND ----------

route_performance_df = (
    df_clean
    .groupBy("event_date", "event_hour", "line_code")
    .agg(
        F.countDistinct("vehicle_prefix").alias("active_vehicles"),
        F.count("*").alias("total_positions"),
        F.avg("latitude").alias("avg_latitude"),
        F.avg("longitude").alias("avg_longitude")
    )
)

print("COUNT route_performance_df:", route_performance_df.count())
route_performance_df.printSchema()
display(route_performance_df.limit(20))

# COMMAND ----------

# COMMAND ----------

route_performance_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("event_date") \
    .save(gold_path)

# COMMAND ----------

# COMMAND ----------

spark.catalog.clearCache()
spark.sql(f"REFRESH TABLE {gold_table}")

print("Dataset gold/route_performance criado com sucesso")
print(f"Table refreshed: {gold_table}")

# COMMAND ----------

# COMMAND ----------

validation_df = spark.read.format("delta").load(gold_path)

display(validation_df.limit(20))
validation_df.printSchema()
print("Total registros validados no path:", validation_df.count())

# COMMAND ----------

# COMMAND ----------

display(
    dbutils.fs.ls("abfss://gold@stspmobilitydev001.dfs.core.windows.net/route_performance")
)

# COMMAND ----------

# COMMAND ----------

print("Total registros via catálogo:", spark.table(gold_table).count())
spark.table(gold_table).printSchema()
