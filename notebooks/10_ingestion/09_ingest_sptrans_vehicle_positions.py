# Databricks notebook source
# MAGIC %run "/Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/notebooks/00_setup/00_adls_gen2_oauth_connection"

# COMMAND ----------

# MAGIC %run "/Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/notebooks/00_setup/00_config"

# COMMAND ----------

print("🚀 Iniciando ingestão SPTrans...")

# ==============================
# CONFIG
# ==============================

scope = "kv-sp-mobility"

dbutils.fs.mkdirs(landing_path)

print("📂 Landing path:", landing_path)

# ==============================
# AUTH SPTRANS
# ==============================

sptrans_token = dbutils.secrets.get(
    scope=scope,
    key="sptrans-api-token"
)

print("🔐 Token carregado")

# ==============================
# API CALL
# ==============================

import requests

session = requests.Session()

auth_url = f"http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token={sptrans_token}"
auth_response = session.post(auth_url)

if auth_response.status_code != 200:
    raise Exception(f"❌ Falha autenticação: {auth_response.text}")

print("✅ Autenticação OK")

position_url = "http://api.olhovivo.sptrans.com.br/v2.1/Posicao"
position_response = session.get(position_url)

if position_response.status_code != 200:
    raise Exception(f"❌ Falha API: {position_response.text}")

print("✅ Coleta OK")

# ==============================
# PROCESSAMENTO
# ==============================

import json
from datetime import datetime

position_data = position_response.json()

capture_ts = datetime.now()
capture_ts_str = capture_ts.strftime("%Y%m%d_%H%M%S")
date_partition = capture_ts.strftime("%Y/%m/%d")

print("🕒 Timestamp:", capture_ts_str)

# ==============================
# LOCAL WRITE
# ==============================

local_file = f"/tmp/sptrans_vehicle_positions_{capture_ts_str}.json"

with open(local_file, "w", encoding="utf-8") as f:
    json.dump(position_data, f, ensure_ascii=False)

print("💾 Salvo local:", local_file)

# ==============================
# DATA LAKE WRITE
# ==============================

target_path = f"{landing_path}/{date_partition}/"
target_file = f"{target_path}sptrans_vehicle_positions_{capture_ts_str}.json"

dbutils.fs.mkdirs(target_path)

dbutils.fs.cp(
    f"file:{local_file}",
    target_file,
    True
)

print("🚀 Enviado para:", target_file)

# ==============================
# DEBUG FINAL
# ==============================

display(dbutils.fs.ls(target_path))

print("✅ INGESTÃO FINALIZADA")
