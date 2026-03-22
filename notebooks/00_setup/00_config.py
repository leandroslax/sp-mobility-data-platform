# Databricks notebook source

# COMMAND ----------

print("⚙️ Loading global configuration...")

# COMMAND ----------
# Storage config

account_name = "stspmobilitydev001"
account_fqdn = f"{account_name}.dfs.core.windows.net"

container_bronze = "bronze"
container_silver = "silver"
container_gold = "gold"

base_path_bronze = f"abfss://{container_bronze}@{account_fqdn}"
base_path_silver = f"abfss://{container_silver}@{account_fqdn}"
base_path_gold = f"abfss://{container_gold}@{account_fqdn}"

# COMMAND ----------
# Spark performance tuning (AUTO SCALE)

num_cores = spark.sparkContext.defaultParallelism

spark.conf.set("spark.sql.shuffle.partitions", num_cores)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

print(f"⚡ Spark tuned with {num_cores} partitions")

# COMMAND ----------

print("✅ Config loaded successfully")
