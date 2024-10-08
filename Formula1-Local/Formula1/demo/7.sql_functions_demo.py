# Databricks notebook source
# MAGIC %sql
# MAGIC USE f1_processsed;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM drivers;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Using CONCAT
# MAGIC SELECT *, CONCAT(driver_ref, '-',code ) AS new_drivers
# MAGIC FROM drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC --Using SPLIT
# MAGIC SELECT *, SPLIT(name, ' ')[0] AS forename, SPLIT(name, ' ')[1] AS surname
# MAGIC FROM drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, current_timestamp
# MAGIC FROM drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, date_format(dob, 'dd-MM-yyyy')
# MAGIC FROM drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, date_add(dob, 1)
# MAGIC FROM drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT nationality, name, dob, RANK() OVER(PARTITION BY nationality ORDER BY dob DESC) AS age_rank
# MAGIC FROM drivers
# MAGIC ORDER BY nationality, age_rank