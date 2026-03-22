# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
scope = "kv-sp-mobility"

client_id = dbutils.secrets.get(scope=scope, key="databricks-sp-client-id")
client_secret = dbutils.secrets.get(scope=scope, key="databricks-sp-secret")
tenant_id = dbutils.secrets.get(scope=scope, key="databricks-sp-tenant-id")

print("client_id_prefix =", client_id[:8])
print("client_id_length =", len(client_id))
print("tenant_id =", tenant_id)

configs = {
    f"fs.azure.account.auth.type.{account_fqdn}": "OAuth",
    f"fs.azure.account.oauth.provider.type.{account_fqdn}": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    f"fs.azure.account.oauth2.client.id.{account_fqdn}": client_id,
    f"fs.azure.account.oauth2.client.secret.{account_fqdn}": client_secret,
    f"fs.azure.account.oauth2.client.endpoint.{account_fqdn}": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

for k, v in configs.items():
    spark.conf.set(k, v)

hc = spark._jsc.hadoopConfiguration()
for k, v in configs.items():
    hc.set(k, v)

hc.unset(f"fs.azure.account.key.{account_fqdn}")

print("OAuth configurado para:", account_fqdn)
print("auth.type =", spark.conf.get(f"fs.azure.account.auth.type.{account_fqdn}"))
print("provider =", spark.conf.get(f"fs.azure.account.oauth.provider.type.{account_fqdn}"))

# COMMAND ----------


client_id = dbutils.secrets.get(scope="kv-sp-mobility", key="databricks-sp-client-id")
client_secret = dbutils.secrets.get(scope="kv-sp-mobility", key="databricks-sp-secret")
tenant_id = dbutils.secrets.get(scope="kv-sp-mobility", key="databricks-sp-tenant-id")

configs = {
    f"fs.azure.account.auth.type.{account_fqdn}": "OAuth",
    f"fs.azure.account.oauth.provider.type.{account_fqdn}": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    f"fs.azure.account.oauth2.client.id.{account_fqdn}": client_id,
    f"fs.azure.account.oauth2.client.secret.{account_fqdn}": client_secret,
    f"fs.azure.account.oauth2.client.endpoint.{account_fqdn}": f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
}

for k, v in configs.items():
    spark.conf.set(k, v)

hc = spark._jsc.hadoopConfiguration()
for k, v in configs.items():
    hc.set(k, v)

hc.unset(f"fs.azure.account.key.{account_fqdn}")

dbutils.fs.ls("abfss://landing@stspmobilitydev001.dfs.core.windows.net/")

# COMMAND ----------

scope = "kv-sp-mobility"
tenant_id = dbutils.secrets.get(scope=scope, key="databricks-sp-tenant-id")
print(tenant_id)
