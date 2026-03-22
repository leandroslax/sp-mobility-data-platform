# Databricks notebook source
# COMMAND ----------



scope = "kv-sp-mobility"


dbutils.fs.mkdirs(landing_path)

print("Landing path:", landing_path)

sptrans_token = dbutils.secrets.get(
    scope=scope,
    key="sptrans-api-token"
)

print("Token carregado com sucesso.")

import requests

session = requests.Session()

auth_url = f"http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token={sptrans_token}"

auth_response = session.post(auth_url)

print("Status autenticação:", auth_response.status_code)
print("Resposta:", auth_response.text)

position_url = "http://api.olhovivo.sptrans.com.br/v2.1/Posicao"

position_response = session.get(position_url)

print("Status posição:", position_response.status_code)

import json
from datetime import datetime

position_data = position_response.json()

print("Chaves do payload:", position_data.keys())
print("Timestamp de captura:", datetime.now().isoformat())

capture_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
local_file = f"/tmp/sptrans_vehicle_positions_{capture_ts}.json"

with open(local_file, "w", encoding="utf-8") as f:
    json.dump(position_data, f, ensure_ascii=False)

print("Arquivo salvo localmente:", local_file)

target_file = f"{landing_path}/sptrans_vehicle_positions_{capture_ts}.json"

dbutils.fs.cp(
    f"file:{local_file}",
    target_file,
    True
)

print("Arquivo enviado para:", target_file)

display(dbutils.fs.ls(landing_path))
