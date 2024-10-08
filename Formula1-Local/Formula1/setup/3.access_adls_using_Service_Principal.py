# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Like using access keys
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/password for the Application
# MAGIC 3. Set Spark Config with App/Client id, Directory/ Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------


client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-demo-Service-Principal-Client-ID')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-demo-Service-Principal-TenantID')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-demo-Service-Principal-Client-Secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlcourseev.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlcourseev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlcourseev.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlcourseev.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlcourseev.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlcourseev.dfs.core.windows.net"))


# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlcourseev.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@formula1dlcourseev.dfs.core.windows.net"))