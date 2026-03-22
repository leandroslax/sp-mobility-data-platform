# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
# MAGIC %run ../00_setup/00_adls_gen2_oauth_connection

<<<<<<< HEAD
scope = "kv-sp-mobility"


dbutils.fs.mkdirs(landing_path)

print("Landing path:", landing_path)

sptrans_token = dbutils.secrets.get(
    scope=scope,
    key="sptrans-api-token"
)

print("Token carregado com sucesso.")
=======
# COMMAND ----------
# ==============================
# INGEST SPTRANS VEHICLE POSITIONS
# ==============================
>>>>>>> 6940db4 (fix: remove invalid inline Databricks notebook headers)

import requests
import json
from datetime import datetime

# Configuration
landing_path = f"abfss://landing@{account_fqdn}/sptrans/vehicle_positions"

print(f"🚀 Starting ingestion to: {landing_path}")

try:
    # Ensure landing path exists
    dbutils.fs.mkdirs(landing_path)

    # Load SPTrans API Token
    sptrans_token = dbutils.secrets.get(scope=secret_scope, key="sptrans-api-token")
    print("✅ SPTrans token loaded")

    # Authenticate with SPTrans API
    session = requests.Session()
    auth_url = f"http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token={sptrans_token}"
    auth_response = session.post(auth_url)

    if auth_response.status_code != 200 or auth_response.text.lower() != "true":
        raise Exception(f"Authentication failed: {auth_response.status_code} - {auth_response.text}")
    
    print("✅ Authenticated with SPTrans API")

    # Fetch Vehicle Positions
    position_url = "http://api.olhovivo.sptrans.com.br/v2.1/Posicao"
    position_response = session.get(position_url)

    if position_response.status_code != 200:
        raise Exception(f"Failed to fetch positions: {position_response.status_code}")

    position_data = position_response.json()
    print(f"✅ Data fetched. Keys: {list(position_data.keys())}")

    # Save locally first (Databricks best practice for small API payloads)
    capture_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_file = f"/tmp/sptrans_vehicle_positions_{capture_ts}.json"

    with open(local_file, "w", encoding="utf-8") as f:
        json.dump(position_data, f, ensure_ascii=False)

    # Copy to ADLS Gen2
    target_file = f"{landing_path}/sptrans_vehicle_positions_{capture_ts}.json"
    dbutils.fs.cp(f"file:{local_file}", target_file)

    print(f"🎯 Ingestion completed successfully: {target_file}")
    display(dbutils.fs.ls(landing_path))

except Exception as e:
    print(f"❌ Error during ingestion: {str(e)}")
    raise e
