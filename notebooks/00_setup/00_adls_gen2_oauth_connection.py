# Databricks notebook source
# ==============================
# ADLS GEN2 OAUTH CONNECTION
# ==============================

print("🔐 Configurando acesso ao ADLS Gen2 via OAuth...")

# ==============================
# CONFIG
# ==============================

scope = "kv-sp-mobility"

account_name = "stpmobilitydev001"
account_fqdn = f"{account_name}.dfs.core.windows.net"

# ==============================
# SECRETS (SERVICE PRINCIPAL)
# ==============================

client_id = dbutils.secrets.get(scope=scope, key="databricks-sp-client-id")
client_secret = dbutils.secrets.get(scope=scope, key="databricks-sp-secret")
tenant_id = dbutils.secrets.get(scope=scope, key="databricks-sp-tenant-id")

print("client_id_prefix =", client_id[:8])
print("tenant_id =", tenant_id)

# ==============================
# OAUTH CONFIG (CORRETO)
# ==============================

configs = {
    f"fs.azure.account.auth.type.{account_fqdn}": "OAuth",
    f"fs.azure.account.oauth.provider.type.{account_fqdn}": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    f"fs.azure.account.oauth2.client.id.{account_fqdn}": client_id,
    f"fs.azure.account.oauth2.client.secret.{account_fqdn}": client_secret,
    f"fs.azure.account.oauth2.client.endpoint.{account_fqdn}": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# aplica configs no spark
for k, v in configs.items():
    spark.conf.set(k, v)

print("✅ OAuth configurado com sucesso")

# ==============================
# DEBUG CONFIG
# ==============================

print("auth.type =", spark.conf.get(f"fs.azure.account.auth.type.{account_fqdn}"))

# ==============================
# TESTE DE ACESSO
# ==============================

print("🔎 Testando acesso ao Data Lake...")

try:
    test_path = f"abfss://bronze@{account_fqdn}/"
    files = dbutils.fs.ls(test_path)

    print("✅ Conexão com ADLS OK")
    print("📂 Conteúdo:")
    
    display(files)

except Exception as e:
    print("❌ Erro ao acessar ADLS:")
    print(e)

# COMMAND ----------

spark.conf.get(f"fs.azure.account.auth.type.{account_fqdn}")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@stpmobilitydev001.dfs.core.windows.net/")
