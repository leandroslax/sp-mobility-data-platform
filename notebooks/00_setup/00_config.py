# Databricks notebook source
# COMMAND ----------

# ==============================
# GLOBAL CONFIG
# ==============================

# Storage Account Configuration
storage_account = "stspmobilitydev001"
account_fqdn = f"{storage_account}.dfs.core.windows.net"

# Base Paths
bronze_base_path = f"abfss://bronze@{account_fqdn}/"
silver_base_path = f"abfss://silver@{account_fqdn}/"
gold_base_path   = f"abfss://gold@{account_fqdn}/"

# Secret Scope
secret_scope = "kv-sp-mobility"

print(f"✅ Global config loaded for storage: {storage_account}")
