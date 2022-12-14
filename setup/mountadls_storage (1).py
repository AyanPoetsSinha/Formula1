# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.list("formula1scope")

# COMMAND ----------

dbutils.secrets.get(scope='formula1scope', key="clientId")

# COMMAND ----------

storage_account_name="ayanstorage0001"
client_id=dbutils.secrets.get(scope='formula1scope', key="clientId")
tenant_id=dbutils.secrets.get(scope='formula1scope', key="tenantId")
client_secret=dbutils.secrets.get(scope='formula1scope', key="clientSecrets")


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{client_id}",
          "fs.azure.account.oauth2.client.secret": f"{client_secret}",
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}



# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mount_adls("raw")

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

mount_adls("presentation")

# COMMAND ----------

mount_adls("demo")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/ayanstorage0001/processed

# COMMAND ----------


