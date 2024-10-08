# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Like using SAS Token
# MAGIC 1. Set the spark SAS Token
# MAGIC 2. List files from demo container
# MAGIC 3. Read the data from circuits.csv file

# COMMAND ----------

formula1_demo_SAS_Token = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-demo-SAS-Token')


# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlcourseev.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dlcourseev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dlcourseev.dfs.core.windows.net",formula1_demo_SAS_Token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlcourseev.dfs.core.windows.net"))


# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlcourseev.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

