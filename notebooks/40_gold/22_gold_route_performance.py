# Databricks notebook source

# Databricks notebook source

# Databricks

# Databricks notebook source




from pyspark.sql import functions as F

# COMMAND ----------

silver_path = "abfss://silver@stspmobilitydev001dev001.dfs.core.windows.net/sptrans/vehicle_positions"
gold_path = "abfss://gold@stspmobilitydev001dev001.dfs.core.windows.net/route_performance"

# COMMAND ----------



# COMMAND ----------

df = spark.read.format("delta").load(silver_path)

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

print("Dataset gold/route_performance criado com sucesso")
print(f"Delta path atualizado: {gold_path}")

# COMMAND ----------



# COMMAND ----------

validation_df = spark.read.format("delta").load(gold_path)

display(validation_df.limit(20))
validation_df.printSchema()
print("Total registros validados no path:", validation_df.count())

# COMMAND ----------



# COMMAND ----------

display(
    dbutils.fs.ls("abfss://gold@stspmobilitydev001dev001.dfs.core.windows.net/route_performance")
)

# COMMAND ----------



# COMMAND ----------

gold_df = spark.read.format("delta").load(gold_path)

print("Total registros via path:", gold_df.count())
gold_df.printSchema()