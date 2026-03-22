# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
<<<<<<< HEAD
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
=======
# MAGIC %run ./00_config
>>>>>>> 6940db4 (fix: remove invalid inline Databricks notebook headers)

# COMMAND ----------
# ==============================
# ADLS GEN2 OAUTH CONNECTION
# ==============================

print(f"🔗 Configuring OAuth for: {account_fqdn}")

try:
    # Retrieve secrets from Azure Key Vault (via Databricks Secret Scope)
    client_id = dbutils.secrets.get(scope=secret_scope, key="databricks-sp-client-id")
    client_secret = dbutils.secrets.get(scope=secret_scope, key="databricks-sp-secret")
    tenant_id = dbutils.secrets.get(scope=secret_scope, key="databricks-sp-tenant-id")

    # OAuth Configurations
    configs = {
        f"fs.azure.account.auth.type.{account_fqdn}": "OAuth",
        f"fs.azure.account.oauth.provider.type.{account_fqdn}": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        f"fs.azure.account.oauth2.client.id.{account_fqdn}": client_id,
        f"fs.azure.account.oauth2.client.secret.{account_fqdn}": client_secret,
        f"fs.azure.account.oauth2.client.endpoint.{account_fqdn}": f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    }

    # Set Spark Configuration
    for k, v in configs.items():
        spark.conf.set(k, v)

    # Set Hadoop Configuration (required for dbutils.fs)
    hc = spark._jsc.hadoopConfiguration()
    for k, v in configs.items():
        hc.set(k, v)

    # Ensure no conflicting account keys are set
    hc.unset(f"fs.azure.account.key.{account_fqdn}")

    print(f"✅ OAuth configured successfully for {account_fqdn}")
    
    # Test connection
    print("🔍 Testing connection to landing container...")
    dbutils.fs.ls(f"abfss://landing@{account_fqdn}/")
    print("✅ Connection test successful")

except Exception as e:
    print(f"❌ Error configuring OAuth: {str(e)}")
    print("💡 Check if the secret scope 'kv-sp-mobility' exists and if the Service Principal has 'Storage Blob Data Contributor' role.")
    raise e
