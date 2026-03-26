# Databricks notebook source
# MAGIC %run "./config"

config = load_config()

client_id = dbutils.secrets.get(
    scope=config["secret_scope"],
    key="sp-mobility-client-id",
)
client_secret = dbutils.secrets.get(
    scope=config["secret_scope"],
    key="sp-mobility-client-secret",
)
tenant_id = dbutils.secrets.get(
    scope=config["secret_scope"],
    key="sp-mobility-tenant-id",
)

spark.conf.set(
    f"fs.azure.account.auth.type.{config['account_fqdn']}",
    "OAuth",
)
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{config['account_fqdn']}",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{config['account_fqdn']}",
    client_id,
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{config['account_fqdn']}",
    client_secret,
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{config['account_fqdn']}",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
)

print(f"OAuth configured for {config['account_fqdn']}")
