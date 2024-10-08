# Databricks notebook source
# MAGIC %md
# MAGIC ### Access dataframe using SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Objectives
# MAGIC ### 1. Create temporary views on dataframe
# MAGIC ### 2. Access the view from SQL cell
# MAGIC ### 3. Access the view from Python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_result")

# COMMAND ----------

#Creating Temp View
race_result_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Check if the table or view exists
# MAGIC SHOW TABLES;
# MAGIC
# MAGIC -- Step 2: Create or import the table if it does not exist
# MAGIC
# MAGIC -- Step 3: Specify the correct database and run the query
# MAGIC SELECT * FROM global_temp.v_race_results WHERE race_year = 2020;

# COMMAND ----------

#Creating Temp View using Python
race_result_2019_df = spark.sql("SELECT * FROM global_temp.v_race_results WHERE race_year = 2019")
display(race_result_2019_df)

# COMMAND ----------

