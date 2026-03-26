# Databricks notebook source
# ==============================
# ADLS GEN2 OAUTH CONNECTION
# ==============================

<<<<<<< HEAD
DEFAULTS_BY_ENV = {
    "dev": {"storage_account": "stspmobilitydev001", "secret_scope": "kv-sp-mobility"},
    "prod": {"storage_account": "stspmobilityprod001", "secret_scope": "kv-sp-mobility"},
}


def _get_widget(name, default_value):
    try:
        dbutils.widgets.text(name, default_value)
    except Exception:
        pass
    try:
        value = dbutils.widgets.get(name)
        return value or default_value
    except Exception:
        return default_value
=======
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
>>>>>>> origin/develop

# ==============================
# DEBUG CONFIG
# ==============================

<<<<<<< HEAD
def load_config():
    env = _get_widget("env", "dev")
    defaults = DEFAULTS_BY_ENV.get(env, DEFAULTS_BY_ENV["dev"])
    storage_account = _get_widget("storage_account", defaults["storage_account"])
    secret_scope = _get_widget("secret_scope", defaults["secret_scope"])
    return {
        "env": env,
        "storage_account": storage_account,
        "account_fqdn": f"{storage_account}.dfs.core.windows.net",
        "secret_scope": secret_scope,
    }

config = load_config()

client_id = dbutils.secrets.get(
    scope=config["secret_scope"],
    key="databricks-sp-client-id",
)
client_secret = dbutils.secrets.get(
    scope=config["secret_scope"],
    key="databricks-sp-secret",
)
tenant_id = dbutils.secrets.get(
    scope=config["secret_scope"],
    key="databricks-sp-tenant-id",
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
=======
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
>>>>>>> origin/develop
