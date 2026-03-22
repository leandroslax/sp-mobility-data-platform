# Databricks notebook source

# COMMAND ----------

# Configurações globais

account_name = "stspmobilitydev001"
account_fqdn = f"{account_name}.dfs.core.windows.net"

container_bronze = "bronze"
container_silver = "silver"
container_gold = "gold"

base_path_bronze = f"abfss://{container_bronze}@{account_fqdn}"
base_path_silver = f"abfss://{container_silver}@{account_fqdn}"
base_path_gold = f"abfss://{container_gold}@{account_fqdn}"

print("✅ Config carregada com sucesso")
