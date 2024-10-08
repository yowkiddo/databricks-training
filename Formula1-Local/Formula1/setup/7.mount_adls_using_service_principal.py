# Databricks notebook source
# MAGIC %md
# MAGIC ### Mouting ADLS using Service Principal
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Set Spark Config with App/Client ID, Directory/Tenant ID & Secret
# MAGIC 3. Call file system utulity mount to mount the storage
# MAGIC 4. Explore other file system utilities related to mount (List all mounts, unmount)

# COMMAND ----------

# DBTITLE 1,_o

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-demo-Service-Principal-Client-ID')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-demo-Service-Principal-TenantID')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-demo-Service-Principal-Client-Secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dlcourseev.dfs.core.windows.net",
  mount_point = "/mnt/formula1dlcourseev/demo",
  extra_configs = configs)


# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dlcourseev/demo"))


# COMMAND ----------

display(spark.read.csv("/mnt/formula1dlcourseev/demo"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

