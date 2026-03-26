# Databricks notebook source
# MAGIC %run /Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/notebooks/00_setup/config

# COMMAND ----------

import json
from datetime import datetime

import requests

config = load_config()
landing_path = config["sptrans_landing_path"]

dbutils.fs.mkdirs(landing_path)

print(f"Landing path: {landing_path}")

sptrans_token = dbutils.secrets.get(
    scope=config["secret_scope"],
    key="sptrans-api-token",
)

session = requests.Session()

auth_url = (
    "http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar"
    f"?token={sptrans_token}"
)
auth_response = session.post(auth_url, timeout=(15, 60))
auth_response.raise_for_status()

position_url = "http://api.olhovivo.sptrans.com.br/v2.1/Posicao"
position_response = session.get(position_url, timeout=(15, 60))
position_response.raise_for_status()

position_data = position_response.json()
capture_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
local_file = f"/tmp/sptrans_vehicle_positions_{capture_ts}.json"

with open(local_file, "w", encoding="utf-8") as handle:
    json.dump(position_data, handle, ensure_ascii=False)

target_file = f"{landing_path}/sptrans_vehicle_positions_{capture_ts}.json"
dbutils.fs.cp(f"file:{local_file}", target_file, True)

print(f"SPTrans snapshot saved to {target_file}")
